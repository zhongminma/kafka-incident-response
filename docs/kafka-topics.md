# Kafka Topics

Step 4 adds repeatable topic setup for the Kafka incident response lab.

## Topics

| Topic | Purpose | Default Partitions | Replication Factor |
| --- | --- | --- | --- |
| `orders.events` | Main event stream consumed by the Kafka-to-database consumer. | `3` | `1` |
| `orders.dlq` | Dead-letter topic for poison messages that cannot be processed safely. | `3` | `1` |

The local Docker Compose Kafka broker is single-node, so the local replication factor is `1`.

## Prerequisite

Start the local dependencies first:

```bash
docker compose up -d
```

## Create Topics

```bash
./scripts/setup-topics.sh
```

The script uses `--if-not-exists`, so it can be run more than once.

## Optional Overrides

```bash
PARTITIONS=6 ./scripts/setup-topics.sh
```

Supported environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `BOOTSTRAP_SERVER` | `localhost:9092` | Kafka bootstrap server used by `/opt/kafka/bin/kafka-topics.sh`. |
| `KAFKA_SERVICE` | `kafka` | Docker Compose service name for the Kafka broker. |
| `PARTITIONS` | `3` | Partition count for both topics. |
| `REPLICATION_FACTOR` | `1` | Replication factor for both topics. |
| `EVENTS_TOPIC` | `orders.events` | Main event topic name. |
| `DLQ_TOPIC` | `orders.dlq` | Dead-letter topic name. |

## Verify Topics

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Expected topics:

```text
orders.dlq
orders.events
```

Describe a topic:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders.events
```

## Step 4 Verification

```bash
sh -n scripts/setup-topics.sh
docker compose config
```

When Kafka is running, also run:

```bash
./scripts/setup-topics.sh
```
