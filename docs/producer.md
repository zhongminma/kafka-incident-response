# Producer MVP

Step 5 adds the first runnable Node.js service: a Kafka producer that publishes valid order events to `orders.events`.

## Purpose

The producer gives later incident scenarios a steady stream of normal messages. It does not create duplicate messages, poison messages, metrics, or scenario controls yet. Those features are intentionally deferred to later steps.

## Event Shape

Each message value is JSON with this shape:

```json
{
  "eventId": "uuid",
  "eventType": "order.created",
  "occurredAt": "2026-06-09T00:00:00.000Z",
  "producer": "orders-producer",
  "payload": {
    "orderId": "uuid",
    "customerId": "customer-1234",
    "sku": "sku-123",
    "quantity": 1,
    "unitPriceCents": 1000,
    "totalCents": 1000
  }
}
```

Kafka message key:

```text
orderId
```

## Run

Start Kafka and create topics first:

```bash
docker compose up -d
./scripts/setup-topics.sh
```

Then run the producer:

```bash
npm run start -w apps/producer
```

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `KAFKA_CLIENT_ID` | `orders-producer` | Kafka client ID. |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list. |
| `EVENTS_TOPIC` | `orders.events` | Topic where normal order events are published. |
| `PRODUCER_INTERVAL_MS` | `1000` | Delay between published events. |
| `DUPLICATE_EVERY_N_MESSAGES` | `0` | Publish the previous event again every N messages; `0` disables duplicates. |
| `POISON_EVERY_N_MESSAGES` | `0` | Publish a poison message every N messages; `0` disables poison messages. |
| `POISON_MODE` | `missing-field` | Poison mode: `missing-field`, `invalid-json`, or `invalid-business`. |

## Step 5 Verification

```bash
npm run check -w apps/producer
npm run test
```

When Kafka is running, also run:

```bash
npm run start -w apps/producer
```

Expected log example:

```text
Producer connected. topic=orders.events brokers=localhost:9092 intervalMs=1000
Published order.created eventId=... orderId=...
```
