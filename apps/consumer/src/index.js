import http from "node:http";
import { Kafka, logLevel } from "kafkajs";
import client from "prom-client";
import pg from "pg";

const { Pool } = pg;

const config = {
  clientId: process.env.KAFKA_CLIENT_ID ?? "orders-consumer",
  groupId: process.env.KAFKA_GROUP_ID ?? "orders-db-writer",
  brokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
  topic: process.env.EVENTS_TOPIC ?? "orders.events",
  dlqTopic: process.env.DLQ_TOPIC ?? "orders.dlq",
  databaseUrl:
    process.env.DATABASE_URL ?? "postgres://app:app@localhost:5432/event_stream",
  processingDelayMs: Number.parseInt(process.env.CONSUMER_PROCESSING_DELAY_MS ?? "0", 10),
  metricsPort: Number.parseInt(process.env.METRICS_PORT ?? "9102", 10)
};

const register = new client.Registry();
client.collectDefaultMetrics({ register, prefix: "consumer_" });

const consumedMessages = new client.Counter({
  name: "consumer_messages_consumed_total",
  help: "Total Kafka messages consumed and stored by the consumer.",
  labelNames: ["topic", "event_type"],
  registers: [register]
});

const duplicateMessages = new client.Counter({
  name: "consumer_duplicate_messages_total",
  help: "Total duplicate Kafka messages skipped by idempotent database writes.",
  labelNames: ["topic", "event_type"],
  registers: [register]
});

const dlqMessages = new client.Counter({
  name: "consumer_dlq_messages_total",
  help: "Total poison messages routed to the dead-letter topic.",
  labelNames: ["source_topic", "dlq_topic", "reason"],
  registers: [register]
});

const consumerErrors = new client.Counter({
  name: "consumer_processing_errors_total",
  help: "Total unexpected consumer message processing errors.",
  labelNames: ["topic"],
  registers: [register]
});

const dbWriteDuration = new client.Histogram({
  name: "consumer_db_write_duration_seconds",
  help: "PostgreSQL write duration in seconds.",
  labelNames: ["topic"],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
  registers: [register]
});

const processingDelayGauge = new client.Gauge({
  name: "consumer_processing_delay_ms",
  help: "Configured artificial processing delay per message.",
  registers: [register]
});
processingDelayGauge.set(config.processingDelayMs);

class ValidationError extends Error {
  constructor(message, reason) {
    super(message);
    this.name = "ValidationError";
    this.reason = reason;
  }
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function parseOrderEvent(messageValue) {
  if (!messageValue) {
    throw new ValidationError("Kafka message value is empty.", "empty-value");
  }

  let event;
  try {
    event = JSON.parse(messageValue.toString("utf8"));
  } catch (error) {
    throw new ValidationError(`Kafka message is not valid JSON: ${error.message}`, "invalid-json");
  }

  if (!event.eventId) {
    throw new ValidationError("Kafka message is missing eventId.", "missing-event-id");
  }

  if (event.eventType !== "order.created") {
    throw new ValidationError("Kafka message has unsupported eventType.", "unsupported-event-type");
  }

  if (!event.payload?.orderId) {
    throw new ValidationError("Kafka message is missing payload.orderId.", "missing-order-id");
  }

  if (!Number.isInteger(event.payload.quantity) || event.payload.quantity <= 0) {
    throw new ValidationError("Kafka message has invalid payload.quantity.", "invalid-quantity");
  }

  if (!Number.isInteger(event.payload.totalCents) || event.payload.totalCents <= 0) {
    throw new ValidationError("Kafka message has invalid payload.totalCents.", "invalid-total");
  }

  return event;
}

async function insertEvent(pool, event, message) {
  const result = await pool.query(
    `INSERT INTO consumed_events (
      event_id,
      event_type,
      order_id,
      topic,
      partition_id,
      message_offset,
      payload
    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (event_id) DO NOTHING
    RETURNING id`,
    [
      event.eventId,
      event.eventType,
      event.payload.orderId,
      message.topic,
      message.partition,
      Number.parseInt(message.message.offset, 10),
      event
    ]
  );

  return result.rowCount === 1;
}

async function routeToDlq(dlqProducer, messageContext, error) {
  const { topic, partition, message } = messageContext;
  const reason = error instanceof ValidationError ? error.reason : "unexpected-error";

  const dlqPayload = {
    sourceTopic: topic,
    sourcePartition: partition,
    sourceOffset: message.offset,
    key: message.key?.toString("utf8") ?? null,
    value: message.value?.toString("utf8") ?? null,
    reason,
    error: error.message,
    failedAt: new Date().toISOString()
  };

  await dlqProducer.send({
    topic: config.dlqTopic,
    messages: [
      {
        key: dlqPayload.key ?? `${topic}-${partition}-${message.offset}`,
        value: JSON.stringify(dlqPayload)
      }
    ]
  });

  dlqMessages.inc({ source_topic: topic, dlq_topic: config.dlqTopic, reason });
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
    console.log(`Consumer metrics listening on port ${config.metricsPort}`);
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

  const consumer = kafka.consumer({ groupId: config.groupId });
  const dlqProducer = kafka.producer();
  const pool = new Pool({ connectionString: config.databaseUrl });
  let shuttingDown = false;

  const shutdown = async (signal) => {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;
    console.log(`Received ${signal}. Disconnecting consumer, DLQ producer, and database pool...`);
    metricsServer.close();
    await consumer.disconnect();
    await dlqProducer.disconnect();
    await pool.end();
    console.log("Consumer disconnected.");
  };

  process.once("SIGINT", () => {
    void shutdown("SIGINT");
  });

  process.once("SIGTERM", () => {
    void shutdown("SIGTERM");
  });

  await pool.query("SELECT 1");
  await dlqProducer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: config.topic, fromBeginning: true });

  console.log(
    `Consumer connected. topic=${config.topic} dlqTopic=${config.dlqTopic} groupId=${config.groupId} brokers=${config.brokers.join(",")} processingDelayMs=${config.processingDelayMs}`
  );

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const event = parseOrderEvent(message.value);

        if (config.processingDelayMs > 0) {
          await sleep(config.processingDelayMs);
        }

        const endTimer = dbWriteDuration.startTimer({ topic });
        const inserted = await insertEvent(pool, event, { topic, partition, message });
        endTimer();

        if (inserted) {
          consumedMessages.inc({ topic, event_type: event.eventType });
          console.log(
            `Stored ${event.eventType} eventId=${event.eventId} orderId=${event.payload.orderId} offset=${message.offset}`
          );
        } else {
          duplicateMessages.inc({ topic, event_type: event.eventType });
          console.log(
            `Skipped duplicate ${event.eventType} eventId=${event.eventId} orderId=${event.payload.orderId} offset=${message.offset}`
          );
        }
      } catch (error) {
        if (error instanceof ValidationError) {
          await routeToDlq(dlqProducer, { topic, partition, message }, error);
          console.log(`Routed poison message to ${config.dlqTopic} offset=${message.offset} reason=${error.reason}`);
          return;
        }

        consumerErrors.inc({ topic });
        throw error;
      }
    }
  });
}

function validateConfig() {
  if (config.brokers.length === 0 || config.brokers.some((broker) => broker.trim() === "")) {
    throw new Error("KAFKA_BROKERS must contain at least one broker.");
  }

  if (!config.databaseUrl) {
    throw new Error("DATABASE_URL is required.");
  }

  if (!Number.isInteger(config.processingDelayMs) || config.processingDelayMs < 0) {
    throw new Error("CONSUMER_PROCESSING_DELAY_MS must be a non-negative integer.");
  }

  if (!Number.isInteger(config.metricsPort) || config.metricsPort <= 0) {
    throw new Error("METRICS_PORT must be a positive integer.");
  }
}

main().catch((error) => {
  console.error("Consumer failed.", error);
  process.exitCode = 1;
});
