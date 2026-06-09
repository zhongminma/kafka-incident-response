import { randomUUID } from "node:crypto";
import { Kafka, logLevel } from "kafkajs";

const config = {
  clientId: process.env.KAFKA_CLIENT_ID ?? "orders-producer",
  brokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
  topic: process.env.EVENTS_TOPIC ?? "orders.events",
  intervalMs: Number.parseInt(process.env.PRODUCER_INTERVAL_MS ?? "1000", 10)
};

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

async function main() {
  validateConfig();

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

    await producer.send({
      topic: config.topic,
      messages: [
        {
          key: event.payload.orderId,
          value: JSON.stringify(event)
        }
      ]
    });

    console.log(
      `Published ${event.eventType} eventId=${event.eventId} orderId=${event.payload.orderId}`
    );

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
}

main().catch((error) => {
  console.error("Producer failed.", error);
  process.exitCode = 1;
});
