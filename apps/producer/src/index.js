import { randomUUID } from "node:crypto";
import http from "node:http";
import { Kafka, logLevel } from "kafkajs";
import client from "prom-client";

const config = {
  clientId: process.env.KAFKA_CLIENT_ID ?? "orders-producer",
  brokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
  topic: process.env.EVENTS_TOPIC ?? "orders.events",
  intervalMs: Number.parseInt(process.env.PRODUCER_INTERVAL_MS ?? "1000", 10),
  metricsPort: Number.parseInt(process.env.METRICS_PORT ?? "9101", 10)
};

const register = new client.Registry();
client.collectDefaultMetrics({ register, prefix: "producer_" });

const producedMessages = new client.Counter({
  name: "producer_messages_published_total",
  help: "Total Kafka messages published by the producer.",
  labelNames: ["topic", "event_type"],
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
    `Producer connected. topic=${config.topic} brokers=${config.brokers.join(",")} intervalMs=${config.intervalMs}`
  );

  while (!shuttingDown) {
    const event = buildOrderEvent();
    const endTimer = publishDuration.startTimer({ topic: config.topic });

    try {
      await producer.send({
        topic: config.topic,
        messages: [
          {
            key: event.payload.orderId,
            value: JSON.stringify(event)
          }
        ]
      });

      producedMessages.inc({ topic: config.topic, event_type: event.eventType });
      console.log(
        `Published ${event.eventType} eventId=${event.eventId} orderId=${event.payload.orderId}`
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
}

main().catch((error) => {
  console.error("Producer failed.", error);
  process.exitCode = 1;
});
