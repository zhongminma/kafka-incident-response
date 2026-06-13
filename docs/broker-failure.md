# Broker Failure Scenario

Step 11 documents a repeatable Kafka broker failure exercise for the local Docker Compose environment.

## Goal

Simulate Kafka unavailability, observe producer and consumer behavior, then recover the broker.

## Prerequisite

Start the lab and create topics:

```bash
docker compose up -d
./scripts/setup-topics.sh
```

Run producer and consumer in separate terminals:

```bash
npm run start -w apps/producer
npm run start -w apps/consumer
```

## Baseline

Check Kafka status:

```bash
./scripts/kafka-status.sh
```

Check metrics:

```bash
curl http://localhost:9101/metrics
curl http://localhost:9102/metrics
```

## Inject Failure

Stop the broker:

```bash
docker compose stop kafka
```

Expected observations:

- Producer publish attempts fail or retry.
- Consumer disconnects or stops receiving messages.
- `producer_publish_errors_total` increases if the producer is running.
- `consumer_processing_errors_total` may increase depending on timing.

## Recover

Restart the broker:

```bash
docker compose start kafka
```

Wait for health:

```bash
docker compose ps kafka
./scripts/kafka-status.sh
```

Expected recovery:

- Producer reconnects and resumes publishing.
- Consumer group reconnects and resumes consuming.
- Lag may temporarily increase, then drain if consumer throughput is high enough.

## Cleanup

```bash
docker compose down
```

## Step 11 Verification

```bash
sh -n scripts/kafka-status.sh
docker compose config
```
