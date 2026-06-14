import { randomUUID } from "node:crypto";
import http from "node:http";
import { Kafka, logLevel } from "kafkajs";
import client from "prom-client";

const config = {
  clientId: process.env.KAFKA_CLIENT_ID ?? "orders-producer",
  brokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
  topic: process.env.EVENTS_TOPIC ?? "orders.events",
  intervalMs: Number.parseInt(process.env.PRODUCER_INTERVAL_MS ?? "1000", 10),
  metricsPort: Number.parseInt(process.env.METRICS_PORT ?? "9101", 10),
  duplicateEveryNMessages: Number.parseInt(process.env.DUPLICATE_EVERY_N_MESSAGES ?? "0", 10),
  poisonEveryNMessages: Number.parseInt(process.env.POISON_EVERY_N_MESSAGES ?? "0", 10),
  poisonMode: process.env.POISON_MODE ?? "missing-field"
};

const register = new client.Registry();
client.collectDefaultMetrics({ register, prefix: "producer_" });

const producedMessages = new client.Counter({
  name: "producer_messages_published_total",
  help: "Total Kafka messages published by the producer.",
  labelNames: ["topic", "event_type"],
  registers: [register]
});

const duplicateMessages = new client.Counter({
  name: "producer_duplicate_messages_published_total",
  help: "Total duplicate Kafka messages intentionally published by the producer.",
  labelNames: ["topic", "event_type"],
  registers: [register]
});

const poisonMessages = new client.Counter({
  name: "producer_poison_messages_published_total",
  help: "Total poison messages intentionally published by the producer.",
  labelNames: ["topic", "mode"],
  registers: [register]
});

const publishErrors = new client.Counter({
  name: "producer_publish_errors_total",
  help: "Total Kafka publish errors observed by the producer.",
  labelNames: ["topic"],
  registers: [register]
});

const publishDuration = new client.Histogram({
  name: "producer_publish_duration_seconds",
  help: "Kafka publish duration in seconds.",
  labelNames: ["topic"],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
  registers: [register]
});

function buildOrderEvent() {
  const quantity = randomInt(1, 5);
  const unitPriceCents = randomInt(500, 20_000);

  return {
    eventId: randomUUID(),
    eventType: "order.created",
    occurredAt: new Date().toISOString(),
    producer: config.clientId,
    payload: {
      orderId: randomUUID(),
      customerId: `customer-${randomInt(1000, 9999)}`,
      sku: `sku-${randomInt(100, 999)}`,
      quantity,
      unitPriceCents,
      totalCents: quantity * unitPriceCents
    }
  };
}

function buildPoisonMessage() {
  if (config.poisonMode === "invalid-json") {
    return {
      key: randomUUID(),
      value: "{not-valid-json",
      eventType: "invalid-json"
    };
  }

  const event = buildOrderEvent();

  if (config.poisonMode === "missing-field") {
    delete event.payload.orderId;
  } else if (config.poisonMode === "invalid-business") {
    event.payload.quantity = -1;
    event.payload.totalCents = -1;
  }

  return {
    key: event.payload.orderId ?? event.eventId,
    value: JSON.stringify(event),
    eventType: event.eventType
  };
}

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function startMetricsServer() {
  const server = http.createServer(async (request, response) => {
    if (request.url !== "/metrics") {
      response.writeHead(404, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "Not found" }));
      return;
    }

    response.writeHead(200, { "content-type": register.contentType });
    response.end(await register.metrics());
  });

  server.listen(config.metricsPort, () => {
    console.log(`Producer metrics listening on port ${config.metricsPort}`);
  });

  return server;
}

function selectMessageToPublish(state) {
  const shouldPoison =
    config.poisonEveryNMessages > 0 &&
    state.publishedCount > 0 &&
    state.publishedCount % config.poisonEveryNMessages === 0;

  if (shouldPoison) {
    return { ...buildPoisonMessage(), isDuplicate: false, isPoison: true };
  }

  const shouldDuplicate =
    config.duplicateEveryNMessages > 0 &&
    state.lastEvent &&
    state.publishedCount > 0 &&
    state.publishedCount % config.duplicateEveryNMessages === 0;

  if (shouldDuplicate) {
    return {
      key: state.lastEvent.payload.orderId,
      value: JSON.stringify(state.lastEvent),
      eventType: state.lastEvent.eventType,
      event: state.lastEvent,
      isDuplicate: true,
      isPoison: false
    };
  }

  const event = buildOrderEvent();
  state.lastEvent = event;
  return {
    key: event.payload.orderId,
    value: JSON.stringify(event),
    eventType: event.eventType,
    event,
    isDuplicate: false,
    isPoison: false
  };
}

async function main() {
  validateConfig();
  const metricsServer = startMetricsServer();

  const kafka = new Kafka({
    clientId: config.clientId,
    brokers: config.brokers,
    logLevel: logLevel.INFO
  });

  const producer = kafka.producer();
  let shuttingDown = false;
  const state = { publishedCount: 0, lastEvent: null };

  const shutdown = async (signal) => {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;
    console.log(`Received ${signal}. Disconnecting producer...`);
    metricsServer.close();
    await producer.disconnect();
    console.log("Producer disconnected.");
  };

  process.once("SIGINT", () => {
    void shutdown("SIGINT");
  });

  process.once("SIGTERM", () => {
    void shutdown("SIGTERM");
  });

  await producer.connect();
  console.log(
    `Producer connected. topic=${config.topic} brokers=${config.brokers.join(",")} intervalMs=${config.intervalMs} duplicateEveryNMessages=${config.duplicateEveryNMessages} poisonEveryNMessages=${config.poisonEveryNMessages} poisonMode=${config.poisonMode}`
  );

  while (!shuttingDown) {
    const message = selectMessageToPublish(state);
    const endTimer = publishDuration.startTimer({ topic: config.topic });

    try {
      await producer.send({
        topic: config.topic,
        messages: [
          {
            key: message.key,
            value: message.value
          }
        ]
      });

      state.publishedCount += 1;
      producedMessages.inc({ topic: config.topic, event_type: message.eventType });

      if (message.isDuplicate) {
        duplicateMessages.inc({ topic: config.topic, event_type: message.eventType });
      }

      if (message.isPoison) {
        poisonMessages.inc({ topic: config.topic, mode: config.poisonMode });
      }

      console.log(
        `Published ${message.eventType} key=${message.key} duplicate=${message.isDuplicate} poison=${message.isPoison}`
      );
    } catch (error) {
      publishErrors.inc({ topic: config.topic });
      throw error;
    } finally {
      endTimer();
    }

    await sleep(config.intervalMs);
  }
}

function validateConfig() {
  if (config.brokers.length === 0 || config.brokers.some((broker) => broker.trim() === "")) {
    throw new Error("KAFKA_BROKERS must contain at least one broker.");
  }

  if (!Number.isInteger(config.intervalMs) || config.intervalMs <= 0) {
    throw new Error("PRODUCER_INTERVAL_MS must be a positive integer.");
  }

  if (!Number.isInteger(config.metricsPort) || config.metricsPort <= 0) {
    throw new Error("METRICS_PORT must be a positive integer.");
  }

  if (!Number.isInteger(config.duplicateEveryNMessages) || config.duplicateEveryNMessages < 0) {
    throw new Error("DUPLICATE_EVERY_N_MESSAGES must be a non-negative integer.");
  }

  if (!Number.isInteger(config.poisonEveryNMessages) || config.poisonEveryNMessages < 0) {
    throw new Error("POISON_EVERY_N_MESSAGES must be a non-negative integer.");
  }

  if (!["missing-field", "invalid-json", "invalid-business"].includes(config.poisonMode)) {
    throw new Error("POISON_MODE must be one of: missing-field, invalid-json, invalid-business.");
  }
}

main().catch((error) => {
  console.error("Producer failed.", error);
  process.exitCode = 1;
});
