#!/usr/bin/env sh
set -eu

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
KAFKA_SERVICE="${KAFKA_SERVICE:-kafka}"
PARTITIONS="${PARTITIONS:-3}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"
EVENTS_TOPIC="${EVENTS_TOPIC:-orders.events}"
DLQ_TOPIC="${DLQ_TOPIC:-orders.dlq}"

run_kafka_topics() {
  docker compose exec -T "$KAFKA_SERVICE" /opt/kafka/bin/kafka-topics.sh "$@"
}

echo "Waiting for Kafka at ${BOOTSTRAP_SERVER}..."
until run_kafka_topics --bootstrap-server "$BOOTSTRAP_SERVER" --list >/dev/null 2>&1; do
  sleep 2
done

echo "Creating Kafka topics if they do not already exist..."

run_kafka_topics \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create \
  --if-not-exists \
  --topic "$EVENTS_TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION_FACTOR"

run_kafka_topics \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create \
  --if-not-exists \
  --topic "$DLQ_TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION_FACTOR"

echo "Configured topics:"
run_kafka_topics --bootstrap-server "$BOOTSTRAP_SERVER" --list
