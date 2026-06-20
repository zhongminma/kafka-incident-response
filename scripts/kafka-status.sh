#!/usr/bin/env sh
set -eu

KAFKA_SERVICE="${KAFKA_SERVICE:-kafka}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"

echo "Docker Compose service status:"
docker compose ps "$KAFKA_SERVICE"

echo
echo "Kafka topics:"
docker compose exec -T "$KAFKA_SERVICE" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --list

echo
echo "Consumer groups:"
docker compose exec -T "$KAFKA_SERVICE" /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --list
