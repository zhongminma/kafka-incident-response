# Metrics

Step 10 adds Prometheus-format metrics endpoints to the producer and consumer.

## Endpoints

| Service | Default Port | Path |
| --- | --- | --- |
| Producer | `9101` | `/metrics` |
| Consumer | `9102` | `/metrics` |

Override either port with `METRICS_PORT`.

## Producer Metrics

| Metric | Meaning |
| --- | --- |
| `producer_messages_published_total` | Count of published Kafka messages by topic and event type. |
| `producer_publish_errors_total` | Count of publish failures by topic. |
| `producer_publish_duration_seconds` | Publish duration histogram. |
| `producer_duplicate_messages_published_total` | Count of intentionally duplicated messages. |
| `producer_*` default metrics | Node.js process and runtime metrics. |

## Consumer Metrics

| Metric | Meaning |
| --- | --- |
| `consumer_messages_consumed_total` | Count of consumed and stored messages by topic and event type. |
| `consumer_processing_errors_total` | Count of processing failures by topic. |
| `consumer_db_write_duration_seconds` | PostgreSQL write duration histogram. |
| `consumer_processing_delay_ms` | Configured artificial per-message delay. |
| `consumer_duplicate_messages_total` | Count of duplicate messages skipped by idempotent writes. |
| `consumer_*` default metrics | Node.js process and runtime metrics. |

## Verify

Start a service and query metrics:

```bash
curl http://localhost:9101/metrics
curl http://localhost:9102/metrics
```

Static verification:

```bash
npm run check -w apps/producer
npm run check -w apps/consumer
npm run test
```
