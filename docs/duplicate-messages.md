# Duplicate Messages Scenario

Step 12 makes duplicate messages reproducible and handles them with idempotent database writes.

## Goal

Publish the same logical event more than once and prove that PostgreSQL stores it only once.

## How It Works

- Producer can intentionally repeat the previous event with the same `eventId`.
- PostgreSQL enforces `event_id UUID NOT NULL UNIQUE`.
- Consumer inserts with `ON CONFLICT (event_id) DO NOTHING`.
- Consumer increments `consumer_duplicate_messages_total` when a duplicate is skipped.

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

Run the producer with duplicates every third message:

```bash
DUPLICATE_EVERY_N_MESSAGES=3 npm run start -w apps/producer
```

## Verify Database State

```bash
docker compose exec postgres psql -U app -d event_stream \
  -c "SELECT event_id, count(*) FROM consumed_events GROUP BY event_id HAVING count(*) > 1;"
```

Expected result:

```text
(0 rows)
```

## Verify Metrics

```bash
curl http://localhost:9101/metrics | grep producer_duplicate_messages_published_total
curl http://localhost:9102/metrics | grep consumer_duplicate_messages_total
```

Expected observation:

- Producer duplicate counter increases.
- Consumer duplicate counter increases.
- `consumed_events` does not contain duplicate `event_id` rows.

## Step 12 Verification

```bash
npm run check -w apps/producer
npm run check -w apps/consumer
npm run test
docker compose config
```
