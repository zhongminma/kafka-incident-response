# Local End-to-End Test Results

This checkpoint validates the Kafka-to-PostgreSQL pipeline and the four incident scenarios implemented through Step 13.

Tested locally on June 20, 2026 with Docker Desktop, Node.js 22, `apache/kafka:3.7.0`, and `postgres:16-alpine`.

## Baseline Pipeline

- The producer published `order.created` events to `orders.events`.
- The consumer stored valid events in PostgreSQL.
- The `orders-db-writer` consumer group drained to zero lag.
- Producer and consumer Prometheus endpoints returned metrics.

## Consumer Lag

Configuration:

```bash
CONSUMER_PROCESSING_DELAY_MS=1000 npm run start -w apps/consumer
PRODUCER_INTERVAL_MS=50 npm run start -w apps/producer
```

Observed lag grew to approximately 200 messages on each of the three partitions. After restarting the consumer without artificial delay, the consumer group drained back to zero lag.

Result: passed.

## Broker Failure

The Kafka container was stopped while the producer and consumer were running.

Observed behavior:

- Producer and consumer logs reported connection resets and refused connections.
- `producer_publish_errors_total` increased.
- The consumer exhausted a retry cycle, restarted itself, and kept retrying.
- After Kafka became healthy, the existing consumer rejoined `orders-db-writer` with all three partitions.
- New events published after recovery were stored in PostgreSQL; the consumed counter increased from 92 to 112 during the first recovery check.

Known limitation: the producer publish loop exits when the KafkaJS retry budget is exhausted. The producer must currently be restarted after a longer broker outage.

Result: consumer recovery passed; automatic producer recovery remains follow-up work.

## Duplicate Messages

Configuration:

```bash
DUPLICATE_EVERY_N_MESSAGES=2 npm run start -w apps/producer
```

Observed behavior:

- `consumer_duplicate_messages_total` reached 28.
- The database unique event constraint prevented duplicate event rows.
- A SQL duplicate event ID check returned zero duplicate groups.

Result: passed.

## Poison Message

Configuration:

```bash
POISON_EVERY_N_MESSAGES=2 POISON_MODE=invalid-json npm run start -w apps/producer
```

Observed behavior:

- `consumer_dlq_messages_total{reason="invalid-json"}` reached 33.
- A DLQ record preserved source topic, partition, offset, original value, failure reason, and timestamp.
- The main consumer continued processing and its group lag returned to zero.

Result: passed.

## Environment Corrections

Live testing found that `bitnami/kafka:3.7` was unavailable. The local environment now uses `apache/kafka:3.7.0`, Apache Kafka environment variable names, and CLI paths under `/opt/kafka/bin`.

The Kafka named volume was removed because the Apache image's application user could not write to the previous mounted path. Local Kafka data is therefore disposable when the container is recreated; PostgreSQL remains persistent through its named volume.
