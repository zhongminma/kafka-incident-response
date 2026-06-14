# Consumer MVP

Step 6 adds a minimal Kafka-to-PostgreSQL consumer.

## Purpose

The consumer reads normal `order.created` events from `orders.events` and inserts them into PostgreSQL.

This step handles duplicate messages idempotently and routes poison messages to `orders.dlq`. It still does not implement retry limits or tracing. Those behaviors are implemented in later steps.

## Database Table

PostgreSQL initializes this table from `infra/docker/postgres/init/001-events.sql`:

```sql
consumed_events
```

Important fields:

| Column | Meaning |
| --- | --- |
| `event_id` | Event UUID from Kafka payload; duplicates are skipped by unique constraint. |
| `event_type` | Event type, currently `order.created`. |
| `order_id` | Business order ID from the event payload. |
| `topic` | Kafka topic name. |
| `partition_id` | Kafka partition number. |
| `message_offset` | Kafka message offset. |
| `payload` | Full original event JSON. |
| `consumed_at` | Database insert timestamp. |

## Run

Start dependencies and create topics first:

```bash
docker compose up -d
./scripts/setup-topics.sh
```

Run the consumer:

```bash
npm run start -w apps/consumer
```

In another terminal, run the producer:

```bash
npm run start -w apps/producer
```

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `KAFKA_CLIENT_ID` | `orders-consumer` | Kafka client ID. |
| `KAFKA_GROUP_ID` | `orders-db-writer` | Kafka consumer group ID. |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list. |
| `EVENTS_TOPIC` | `orders.events` | Topic to consume. |
| `DATABASE_URL` | `postgres://app:app@localhost:5432/event_stream` | PostgreSQL connection string. |

## Verify Inserts

```bash
docker compose exec postgres psql -U app -d event_stream \
  -c "SELECT event_type, count(*) FROM consumed_events GROUP BY event_type;"
```

## Step 6 Verification

```bash
npm run check -w apps/consumer
npm run test
docker compose config
```

When Kafka and PostgreSQL are running, also run the consumer and producer together and verify rows appear in `consumed_events`.
