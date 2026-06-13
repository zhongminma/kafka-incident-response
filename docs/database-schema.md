# Database Schema

Step 7 formalizes the database layer used by the Kafka-to-PostgreSQL consumer MVP.

## Database

| Setting | Value |
| --- | --- |
| Database | `event_stream` |
| User | `app` |
| Local port | `5432` |
| Init path | `infra/docker/postgres/init/001-events.sql` |

## Table: consumed_events

The `consumed_events` table stores each Kafka message successfully written by the consumer.

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | `BIGSERIAL PRIMARY KEY` | Internal database row ID. |
| `event_id` | `UUID NOT NULL UNIQUE` | Event ID from the Kafka payload; used for idempotency. |
| `event_type` | `TEXT NOT NULL` | Event type such as `order.created`. |
| `order_id` | `UUID NOT NULL` | Business order ID from the event payload. |
| `topic` | `TEXT NOT NULL` | Kafka topic name. |
| `partition_id` | `INTEGER NOT NULL` | Kafka partition number. |
| `message_offset` | `BIGINT NOT NULL` | Kafka offset for the consumed message. |
| `payload` | `JSONB NOT NULL` | Full event payload as received from Kafka. |
| `consumed_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | Insert timestamp. |

## Indexes

| Index | Purpose |
| --- | --- |
| `consumed_events_event_type_idx` | Query by event type. |
| `consumed_events_order_id_idx` | Query by order ID. |
| `consumed_events_consumed_at_idx` | Query by ingestion time. |

## Duplicate Handling

Step 12 adds a unique `event_id` constraint and idempotent consumer inserts for duplicate-message handling.

## Verify

Start PostgreSQL first:

```bash
docker compose up -d postgres
```

Inspect the schema:

```bash
./scripts/db-summary.sh
```

Static verification:

```bash
docker compose config
sh -n scripts/db-summary.sh
```
