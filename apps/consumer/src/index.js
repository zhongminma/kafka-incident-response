import { Kafka, logLevel } from "kafkajs";
import pg from "pg";

const { Pool } = pg;

const config = {
  clientId: process.env.KAFKA_CLIENT_ID ?? "orders-consumer",
  groupId: process.env.KAFKA_GROUP_ID ?? "orders-db-writer",
  brokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
  topic: process.env.EVENTS_TOPIC ?? "orders.events",
  databaseUrl:
    process.env.DATABASE_URL ?? "postgres://app:app@localhost:5432/event_stream"
};

function parseOrderEvent(messageValue) {
  if (!messageValue) {
    throw new Error("Kafka message value is empty.");
  }

  const event = JSON.parse(messageValue.toString("utf8"));

  if (!event.eventId || !event.eventType || !event.payload?.orderId) {
    throw new Error("Kafka message is missing required order event fields.");
  }

  return event;
}

async function insertEvent(pool, event, message) {
  await pool.query(
    `INSERT INTO consumed_events (
      event_id,
      event_type,
      order_id,
      topic,
      partition_id,
      message_offset,
      payload
    ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
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
}

async function main() {
  validateConfig();

  const kafka = new Kafka({
    clientId: config.clientId,
    brokers: config.brokers,
    logLevel: logLevel.INFO
  });

  const consumer = kafka.consumer({ groupId: config.groupId });
  const pool = new Pool({ connectionString: config.databaseUrl });
  let shuttingDown = false;

  const shutdown = async (signal) => {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;
    console.log(`Received ${signal}. Disconnecting consumer and database pool...`);
    await consumer.disconnect();
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
  await consumer.connect();
  await consumer.subscribe({ topic: config.topic, fromBeginning: true });

  console.log(
    `Consumer connected. topic=${config.topic} groupId=${config.groupId} brokers=${config.brokers.join(",")}`
  );

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = parseOrderEvent(message.value);

      await insertEvent(pool, event, { topic, partition, message });

      console.log(
        `Stored ${event.eventType} eventId=${event.eventId} orderId=${event.payload.orderId} offset=${message.offset}`
      );
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
}

main().catch((error) => {
  console.error("Consumer failed.", error);
  process.exitCode = 1;
});
