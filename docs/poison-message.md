# Poison Message Scenario

Step 13 makes poison messages reproducible and routes invalid records to `orders.dlq`.

## Goal

Publish invalid Kafka messages without blocking the consumer group.

## Poison Modes

| Mode | Behavior |
| --- | --- |
| `missing-field` | Publishes a JSON event without `payload.orderId`. |
| `invalid-json` | Publishes malformed JSON. |
| `invalid-business` | Publishes an event with invalid negative quantity and total. |

## Run

Start dependencies and topics:

```bash
docker compose up -d
./scripts/setup-topics.sh
```

Run the consumer:

```bash
npm run start -w apps/consumer
```

Run the producer with a poison message every third message:

```bash
POISON_EVERY_N_MESSAGES=3 POISON_MODE=missing-field npm run start -w apps/producer
```

## Verify DLQ

Read records from the dead-letter topic:

```bash
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.dlq \
  --from-beginning \
  --max-messages 5
```

Expected observation:

- Bad messages are written to `orders.dlq`.
- Consumer continues processing later valid messages.
- Invalid events are not inserted into `consumed_events`.

## Verify Metrics

```bash
curl http://localhost:9101/metrics | grep producer_poison_messages_published_total
curl http://localhost:9102/metrics | grep consumer_dlq_messages_total
```

## Step 13 Verification

```bash
npm run check -w apps/producer
npm run check -w apps/consumer
npm run test
docker compose config
```
