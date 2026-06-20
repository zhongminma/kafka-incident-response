# Step-by-Step Testing Guide

Use this guide to validate the Kafka-to-PostgreSQL pipeline and all four incident scenarios.

## Prerequisites

- Docker Desktop is running.
- Node.js 20 or later is installed.
- Project dependencies have been installed with `npm install`.

Open three terminals in the project directory:

```bash
cd /Users/kevinma/Documents/kafka-incident-response
```

| Terminal | Purpose |
| --- | --- |
| A | Docker, Kafka administration, metrics, and database checks |
| B | Consumer process |
| C | Producer process |

## Start the Lab

In Terminal A:

```bash
docker compose up -d
docker compose ps
./scripts/setup-topics.sh
```

Wait until Kafka and PostgreSQL report `healthy`. Topic setup should list `orders.events` and `orders.dlq`.

## Verify the Base Pipeline

In Terminal B:

```bash
npm run start -w apps/consumer
```

In Terminal C:

```bash
npm run start -w apps/producer
```

The Producer should print `Published order.created`, and the Consumer should print `Stored order.created`.

Check PostgreSQL from Terminal A:

```bash
npm run db:summary
```

The pipeline is working when `consumed_event_count` is greater than zero. The value is cumulative because the PostgreSQL volume persists between runs.

Press `Control+C` in Terminal C to stop the Producer before starting the first scenario. Keep the Consumer running unless a scenario says otherwise.

## Scenario 1: Increasing Consumer Lag

Stop the normal Consumer in Terminal B, then restart it with a one-second processing delay:

```bash
CONSUMER_PROCESSING_DELAY_MS=1000 npm run start -w apps/consumer
```

Start a fast Producer in Terminal C:

```bash
PRODUCER_INTERVAL_MS=50 npm run start -w apps/producer
```

After about 20 seconds, inspect lag from Terminal A:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group orders-db-writer
```

The scenario is reproduced when `LAG` is greater than zero. One test observed partition lag values of `139`, `128`, and `152`, for a total of `419`.

Recovery:

1. Stop the fast Producer with `Control+C`.
2. Stop the slow Consumer with `Control+C`.
3. Restart the Consumer without `CONSUMER_PROCESSING_DELAY_MS`.
4. Run the consumer group command again and confirm that every partition returns to `LAG 0`.

## Scenario 2: Broker Failure

Run a normal Consumer in Terminal B and a normal Producer in Terminal C. Stop Kafka from Terminal A:

```bash
docker compose stop kafka
```

Expected client errors include `ECONNRESET`, `ECONNREFUSED`, and `Failed to connect to seed broker`.

Restart Kafka:

```bash
docker compose start kafka
docker compose ps
```

Wait until Kafka reports `healthy`. The Consumer should retry automatically and eventually print `Consumer has joined the group` with all three partitions assigned.

The Producer currently stops publishing if the outage exceeds its KafkaJS retry budget. If it prints `Producer failed. KafkaJSNonRetriableError`, press `Control+C` and restart it:

```bash
npm run start -w apps/producer
```

Recovery is complete when the Producer prints `Published order.created` and the Consumer prints `Stored order.created` again.

## Scenario 3: Duplicate Messages

Keep the normal Consumer running. Stop the previous Producer and start duplicate mode in Terminal C:

```bash
DUPLICATE_EVERY_N_MESSAGES=2 npm run start -w apps/producer
```

The Consumer should print both `Stored order.created` and `Skipped duplicate order.created`. The database `event_id` unique constraint prevents the duplicate insert.

Check the metric from Terminal A:

```bash
curl -s http://localhost:9102/metrics | grep consumer_duplicate_messages_total
```

The scenario passes when the counter is greater than zero. One test observed a value of `85`.

Stop the Producer with `Control+C` when finished.

## Scenario 4: Poison Message

Keep the normal Consumer running. Start invalid-JSON mode in Terminal C:

```bash
POISON_EVERY_N_MESSAGES=2 \
POISON_MODE=invalid-json \
npm run start -w apps/producer
```

The Producer should print both `Published invalid-json` and `Published order.created`. The Consumer should print `Routed poison message to orders.dlq` while continuing to store valid events.

Check the metric:

```bash
curl -s http://localhost:9102/metrics | grep consumer_dlq_messages_total
```

The scenario passes when the counter is greater than zero. One test observed a value of `24`.

Read one DLQ record:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.dlq \
  --from-beginning \
  --max-messages 1
```

The record should preserve `sourceTopic`, `sourcePartition`, `sourceOffset`, the original `value`, `reason`, `error`, and `failedAt`.

## Stop the Lab

1. Stop the Producer with `Control+C`.
2. Stop the Consumer with `Control+C`.
3. Stop the containers from Terminal A:

```bash
docker compose down
```

PostgreSQL data is preserved. To delete all persisted test data, use `docker compose down -v`.

## Troubleshooting

| Problem | Resolution |
| --- | --- |
| Kafka or PostgreSQL is not `healthy` | Wait and run `docker compose ps` again; inspect `docker compose logs kafka` if needed. |
| A required topic is missing | Run `./scripts/setup-topics.sh`. |
| The Consumer has no output | Start the Producer; an idle Consumer waits quietly when no records are available. |
| `EADDRINUSE: 9102` | Stop the existing Consumer before starting another one. |
| `EADDRINUSE: 9101` | Stop the existing Producer before starting another one. |
| Lag does not increase | Confirm `PRODUCER_INTERVAL_MS=50` and `CONSUMER_PROCESSING_DELAY_MS=1000`. |
| Producer does not recover with Kafka | Stop and restart the Producer after Kafka becomes healthy. |
| Consumer has not rejoined yet | Wait for Kafka health and the Consumer retry backoff; a manual restart is normally unnecessary. |
| Kafka CLI command is not found | Use the full `/opt/kafka/bin/kafka-*.sh` path. |
| Database event count is unexpectedly large | The count includes prior runs; reset with `docker compose down -v` only when data deletion is acceptable. |
