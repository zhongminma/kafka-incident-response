# Consumer Lag Scenario

Step 9 makes consumer lag reproducible by letting the producer publish faster than the consumer can write to PostgreSQL.

## Scenario

Lag grows when:

- Producer publish interval is low.
- Consumer processing delay is high.
- The consumer group cannot keep up with the topic write rate.

## Run

Start dependencies and topics:

```bash
docker compose up -d
./scripts/setup-topics.sh
```

Start a slow consumer:

```bash
CONSUMER_PROCESSING_DELAY_MS=1000 npm run start -w apps/consumer
```

Start a faster producer in another terminal:

```bash
PRODUCER_INTERVAL_MS=100 npm run start -w apps/producer
```

## Inspect Lag

```bash
docker compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group orders-db-writer
```

Expected observation:

- `CURRENT-OFFSET` grows slowly.
- `LOG-END-OFFSET` grows quickly.
- `LAG` increases while producer throughput is higher than consumer throughput.

## Recover

Stop the slow consumer and restart without artificial delay:

```bash
npm run start -w apps/consumer
```

Expected observation:

- Consumer catches up.
- Lag eventually returns to `0` if producer throughput stays low enough.

## Step 9 Verification

```bash
npm run check -w apps/consumer
npm run check -w apps/producer
npm run test
```
