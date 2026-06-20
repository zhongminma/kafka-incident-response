# Kafka Reliability & Event Streaming Platform

This project simulates a Kafka-to-database event streaming platform and turns common Kafka incidents into repeatable, observable exercises.

The goal is not to build everything at once. The project will be implemented in small steps. Each step must complete one feature or one goal, and every GitHub commit or pull request requires explicit approval before it is submitted.

## Project Goals

- Build a local Kafka-to-DB pipeline.
- Simulate and diagnose consumer lag growth.
- Simulate broker failure and observe service behavior.
- Simulate duplicate messages and implement idempotent database writes.
- Simulate poison messages and route failed records safely.
- Add observability with Prometheus, Grafana, and OpenTelemetry.
- Provide Kubernetes manifests after the local Docker workflow is stable.
- Keep each implementation step small, reviewable, and easy to roll back.

## Technology Stack

- Kafka for event streaming.
- Node.js for producer, consumer, and control API services.
- Database, initially PostgreSQL, for durable event storage.
- Docker Compose for the first local environment.
- Kubernetes for the deployable platform version.
- Prometheus for metrics collection.
- Grafana for dashboards.
- OpenTelemetry for traces and service instrumentation.

## High-Level Architecture

```mermaid
flowchart LR
    Producer["Node.js Producer"] --> OrdersTopic["Kafka Topic: orders.events"]
    OrdersTopic --> Consumer["Node.js Consumer"]
    Consumer --> Database["PostgreSQL"]
    Consumer --> DLQ["Kafka Topic: orders.dlq"]
    ControlAPI["Node.js Control API"] --> Producer
    ControlAPI --> Consumer
    ControlAPI --> Kafka["Kafka Broker"]
    Producer --> OTel["OpenTelemetry Collector"]
    Consumer --> OTel
    ControlAPI --> OTel
    OTel --> Prometheus["Prometheus"]
    Prometheus --> Grafana["Grafana"]
```

## Core Services

| Service | Purpose |
| --- | --- |
| Producer | Generates business events and publishes them to Kafka. |
| Consumer | Reads Kafka events, validates payloads, and writes records to the database. |
| Control API | Exposes scenario toggles such as slow consumer mode, duplicate publishing, poison message publishing, and health checks. |
| Database | Stores consumed events and supports idempotency checks. |
| Kafka | Provides the event streaming backbone. |
| Prometheus | Scrapes service, Kafka, and database metrics. |
| Grafana | Visualizes lag, throughput, failures, retries, and database writes. |
| OpenTelemetry Collector | Receives traces and metrics from Node.js services. |

## Incident Scenarios

### 1. Consumer Lag Keeps Increasing

The producer emits messages faster than the consumer can process them.

Expected learning goals:

- Understand consumer group lag.
- Compare producer rate, consumer throughput, and database write latency.
- Observe when lag grows, stabilizes, or drains.
- Practice scaling or tuning the consumer.

Possible controls:

- Increase producer event rate.
- Add artificial consumer processing delay.
- Add artificial database write delay.
- Reduce consumer concurrency.

Important signals:

- Kafka consumer group lag.
- Messages produced per second.
- Messages consumed per second.
- Database write latency.
- Consumer error rate.

### 2. Broker Failure

Kafka becomes temporarily unavailable or one broker is stopped.

Expected learning goals:

- Understand producer retry behavior.
- Understand consumer reconnect behavior.
- Observe message delivery recovery after broker availability returns.
- Distinguish service failure from dependency failure.

Possible controls:

- Stop the Kafka broker container.
- Restart the Kafka broker container.
- In Kubernetes, delete a broker pod and watch recovery.

Important signals:

- Producer publish failures.
- Consumer disconnects and reconnects.
- Kafka broker availability.
- Event throughput drop and recovery.
- Application health and readiness state.

### 3. Duplicate Messages

The same logical event is published or consumed more than once.

Expected learning goals:

- Understand at-least-once delivery.
- Use event IDs and database constraints for idempotency.
- Track duplicates without corrupting business state.

Possible controls:

- Publish the same event ID multiple times.
- Force a consumer retry after a successful database write but before offset commit.

Important signals:

- Duplicate event count.
- Database conflict count.
- Consumer retry count.
- Stored event count versus consumed message count.

### 4. Poison Message

A malformed or invalid message blocks normal processing unless it is handled safely.

Expected learning goals:

- Validate messages before database writes.
- Avoid infinite retry loops.
- Route invalid records to a dead-letter topic.
- Preserve enough context for later investigation.

Possible controls:

- Publish invalid JSON.
- Publish a valid JSON record with missing required fields.
- Publish a record that intentionally fails business validation.

Important signals:

- Validation failure count.
- Dead-letter topic message count.
- Consumer retry count.
- Consumer processing latency.

## Step-by-Step Delivery Plan

Each step should be small enough to review independently. A step is complete only when it has documentation, a verification command or checklist, and approval to commit.

| Step | Goal | Feature Scope | Expected Deliverable |
| --- | --- | --- | --- |
| 1 | Define architecture | README with goals, system design, scenarios, and delivery rules | `README.md` |
| 2 | Add local project skeleton | Minimal directories and package metadata only | Empty service folders, root package metadata, no runtime behavior |
| 3 | Start local dependencies | Docker Compose for Kafka and PostgreSQL | `docker-compose.yml` plus setup docs |
| 4 | Create Kafka topic setup | Repeatable topic creation for main and DLQ topics | Script or container command for `orders.events` and `orders.dlq` |
| 5 | Build producer MVP | Producer publishes valid events at a fixed rate | Node.js producer with basic logs |
| 6 | Build consumer MVP | Consumer reads events and writes to database | Node.js consumer with database insert |
| 7 | Add database schema | Durable event table with unique event ID | Migration or init SQL |
| 8 | Add control API | Basic endpoints for health and scenario toggles | Node.js API service |
| 9 | Simulate increasing lag | Producer rate or consumer delay control | Reproducible lag scenario |
| 10 | Add lag metrics | Expose consumer throughput and lag-related metrics | Prometheus metrics endpoint |
| 11 | Simulate broker failure | Documented broker stop/restart workflow | Runbook section and verification checklist |
| 12 | Handle duplicate messages | Idempotent database writes using event ID | Duplicate scenario and metrics |
| 13 | Handle poison messages | Validation plus dead-letter topic routing | DLQ flow and investigation docs |
| 14 | Add OpenTelemetry traces | Trace producer, consumer, API, and DB operations | OTel collector config and trace spans |
| 15 | Add Prometheus | Scrape Node.js services and infrastructure metrics | Prometheus config |
| 16 | Add Grafana dashboard | Dashboard for throughput, lag, errors, and DLQ | Grafana dashboard JSON |
| 17 | Add Kubernetes manifests | Deploy Kafka, DB, services, and observability components | Kubernetes YAML under `k8s/` |
| 18 | Add incident runbooks | Operator instructions for each failure mode | Docs for detect, diagnose, mitigate, recover |
| 19 | Add tests | Unit or integration tests for critical behavior | Tests for idempotency, validation, and health |
| 20 | Final review | Polish docs, verify all scenarios, prepare demo flow | Complete demo checklist |

## GitHub Approval Rule

No commit, branch push, pull request, or GitHub issue update should be performed automatically.

Workflow for every step:

1. Implement or document exactly one step.
2. Show the changed files and summarize the result.
3. Run the step's verification checklist where applicable.
4. Ask for approval before any Git commit.
5. Ask for approval again before any push or pull request.

## Initial Local Development Strategy

The recommended order is local-first:

1. Build the Docker Compose environment.
2. Prove Kafka-to-DB delivery locally.
3. Add incident toggles one at a time.
4. Add metrics and dashboards.
5. Move the stable version into Kubernetes.

This keeps early debugging fast and avoids mixing application behavior problems with Kubernetes deployment problems.

## Repository Layout

The repository starts as a small Node.js workspace. Runtime code will be added in later steps.

```text
docker-compose.yml  Root Compose entrypoint for local dependencies.
apps/
  producer/       Kafka event producer service.
  consumer/       Kafka-to-database consumer service.
  control-api/    Scenario control and health API.
packages/
  shared/         Shared event contracts and helper utilities.
infra/
  docker/         Docker Compose and local dependency configuration.
  kubernetes/     Kubernetes manifests.
  observability/  Prometheus, Grafana, and OpenTelemetry configuration.
docs/             Incident runbooks and verification notes.
scripts/          Local setup and maintenance scripts.
```

Step 2 verification:

- `git status --short` shows only skeleton files for review.
- `find . -maxdepth 3 -type f | sort` shows workspace metadata and placeholder files only.
- No application runtime code has been added.

Step 3 verification:

- `docker compose config` resolves the root Compose entrypoint.
- Kafka is configured as a local single-node broker on port `9092`.
- PostgreSQL is configured on port `5432` with database `event_stream`.

Step 4 verification:

- `sh -n scripts/setup-topics.sh` validates the topic setup script syntax.
- `docker compose config` confirms the Docker Compose service name used by the script.
- When Kafka is running, `./scripts/setup-topics.sh` creates `orders.events` and `orders.dlq` idempotently.

Step 5 verification:

- `npm run check -w apps/producer` validates producer JavaScript syntax.
- `npm run test` verifies workspace test scripts still resolve.
- When Kafka is running, `npm run start -w apps/producer` publishes normal `order.created` events to `orders.events`.

Step 6 verification:

- `npm run check -w apps/consumer` validates consumer JavaScript syntax.
- `npm run test` verifies workspace test scripts still resolve.
- `docker compose config` confirms the PostgreSQL init schema mount resolves correctly.
- When Kafka and PostgreSQL are running, the consumer stores normal `order.created` events in `consumed_events`.

Step 7 verification:

- `sh -n scripts/db-summary.sh` validates the database summary script syntax.
- `npm run db:summary` inspects the local database when PostgreSQL is running.
- `docker compose config` confirms the PostgreSQL init schema mount still resolves correctly.

Step 8 verification:

- `npm run check -w apps/control-api` validates Control API JavaScript syntax.
- `npm run test` verifies workspace test scripts still resolve.
- When running locally, `GET /health` returns service health and `GET /scenarios` returns scenario state.

Step 9 verification:

- `npm run check -w apps/consumer` validates the artificial processing delay control.
- `npm run check -w apps/producer` validates producer interval configuration.
- `docs/consumer-lag.md` documents how to create and recover increasing lag.

Step 10 verification:

- `npm run check -w apps/producer` validates producer metrics endpoint code.
- `npm run check -w apps/consumer` validates consumer metrics endpoint code.
- `docs/metrics.md` lists producer and consumer Prometheus metrics.

Step 11 verification:

- `sh -n scripts/kafka-status.sh` validates the Kafka status script syntax.
- `docker compose config` confirms the Kafka service remains resolvable.
- `docs/broker-failure.md` documents stop, observe, recover, and cleanup steps.

Step 12 verification:

- `npm run check -w apps/producer` validates duplicate publishing controls.
- `npm run check -w apps/consumer` validates idempotent consumer writes.
- `docker compose config` confirms the updated schema remains mounted.
- `docs/duplicate-messages.md` documents duplicate injection and verification.

Step 13 verification:

- `npm run check -w apps/producer` validates poison publishing controls.
- `npm run check -w apps/consumer` validates message validation and DLQ routing.
- `docker compose config` confirms the DLQ topic remains available through Kafka setup.
- `docs/poison-message.md` documents poison injection and DLQ verification.

Local integration checkpoint:

- `docs/end-to-end-testing.md` records live results for the baseline pipeline and all four incident scenarios.
- Consumer lag growth and recovery, duplicate protection, and poison-message DLQ routing passed.
- Broker failure detection and consumer recovery passed; automatic producer recovery remains follow-up work.

## Step-by-Step Testing Guide

Follow this section from top to bottom to validate the base Kafka-to-database pipeline and all four incident scenarios.

### 1. Prepare Three Terminals

Open three terminal windows:

| Terminal | Purpose |
| --- | --- |
| Terminal A | Docker, Kafka administration, metrics, and database checks |
| Terminal B | Run the Consumer |
| Terminal C | Run the Producer |

In every terminal, change to the project directory:

```bash
cd /Users/kevinma/Documents/kafka-incident-response
```

### 2. Start the Dependencies

In Terminal A, run:

```bash
docker compose up -d
docker compose ps
```

Confirm that Kafka and PostgreSQL both report `healthy`:

```text
kafka-incident-response-kafka      Up ... (healthy)
kafka-incident-response-postgres   Up ... (healthy)
```

Create the Kafka topics:

```bash
./scripts/setup-topics.sh
```

The output should include:

```text
orders.dlq
orders.events
```

### 3. Verify the Base Kafka-to-DB Pipeline

Start the Consumer in Terminal B:

```bash
npm run start -w apps/consumer
```

Successful startup looks like this:

```text
Consumer metrics listening on port 9102
Consumer connected. topic=orders.events ... processingDelayMs=0
Consumer has joined the group
```

Start the Producer in Terminal C:

```bash
npm run start -w apps/producer
```

The Producer should continuously print:

```text
Published order.created ... duplicate=false poison=false
```

The Consumer should continuously print:

```text
Stored order.created ...
```

Check the database from Terminal A:

```bash
npm run db:summary
```

If `consumed_event_count` is greater than zero, events have successfully traveled through the Producer, Kafka, and Consumer into PostgreSQL. This value is cumulative and does not need to match the number of events published during the current test.

When finished, press `Control+C` in Terminal C to stop the Producer. Keep the Consumer running in Terminal B.

### 4. Scenario One: Increasing Consumer Lag

Press `Control+C` in Terminal B to stop the normal Consumer, then start a slow Consumer:

```bash
CONSUMER_PROCESSING_DELAY_MS=1000 npm run start -w apps/consumer
```

Confirm that the log shows:

```text
processingDelayMs=1000
```

Start a fast Producer in Terminal C:

```bash
PRODUCER_INTERVAL_MS=50 npm run start -w apps/producer
```

Wait about 20 seconds, then inspect lag from Terminal A:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group orders-db-writer
```

The scenario is reproduced when `LAG` is greater than zero on all three partitions. One local test produced:

```text
PARTITION 0  LAG 139
PARTITION 1  LAG 128
PARTITION 2  LAG 152
```

The total lag was `419`.

Recovery steps:

1. Press `Control+C` in Terminal C to stop the fast Producer.
2. Press `Control+C` in Terminal B to stop the slow Consumer.
3. Run `npm run start -w apps/consumer` in Terminal B to restore normal processing speed.
4. Run the consumer group command again and confirm that `LAG` returns to `0` on all three partitions.

### 5. Scenario Two: Broker Failure

Make sure the normal Consumer is running in Terminal B and the normal Producer is running in Terminal C.

Terminal B:

```bash
npm run start -w apps/consumer
```

Terminal C:

```bash
npm run start -w apps/producer
```

Stop Kafka from Terminal A:

```bash
docker compose stop kafka
```

The Producer and Consumer should report one or more of these errors:

```text
ECONNRESET
ECONNREFUSED
Failed to connect to seed broker
```

These errors confirm that the clients detected the broker failure.

Restart Kafka:

```bash
docker compose start kafka
docker compose ps
```

Wait until Kafka reports `healthy` again. The Consumer should retry automatically and rejoin its consumer group. Successful recovery looks like this:

```text
Consumer has joined the group
memberAssignment={"orders.events":[0,1,2]}
```

The Producer currently has a known limitation. If Kafka remains unavailable longer than the KafkaJS retry budget, the Producer reports:

```text
Producer failed. KafkaJSNonRetriableError
```

To recover, press `Control+C` in Terminal C and restart the Producer:

```bash
npm run start -w apps/producer
```

The system has recovered when the Producer prints `Published order.created` and the Consumer prints `Stored order.created` again.

### 6. Scenario Three: Duplicate Messages

Keep the normal Consumer running, stop the previous Producer, and start duplicate-message mode in Terminal C:

```bash
DUPLICATE_EVERY_N_MESSAGES=2 npm run start -w apps/producer
```

The Consumer should print both:

```text
Stored order.created ...
Skipped duplicate order.created ...
```

The first occurrence of an `eventId` is stored. The second occurrence is identified as a duplicate, and the database `event_id` unique constraint prevents another insert.

Check the metric from Terminal A:

```bash
curl -s http://localhost:9102/metrics | grep consumer_duplicate_messages_total
```

One local test produced:

```text
consumer_duplicate_messages_total{topic="orders.events",event_type="order.created"} 85
```

The scenario passes when this counter is greater than zero and the Consumer prints `Skipped duplicate`. Press `Control+C` in Terminal C when finished.

### 7. Scenario Four: Poison Message

Keep the normal Consumer running and start invalid-JSON mode in Terminal C:

```bash
POISON_EVERY_N_MESSAGES=2 \
POISON_MODE=invalid-json \
npm run start -w apps/producer
```

The Producer should alternate between poison messages and normal events:

```text
Published invalid-json ... poison=true
Published order.created ... poison=false
```

The Consumer should print:

```text
Routed poison message to orders.dlq ... reason=invalid-json
Stored order.created ...
```

This confirms that bad records are routed to the DLQ while valid records continue to be processed.

Check the DLQ metric:

```bash
curl -s http://localhost:9102/metrics | grep consumer_dlq_messages_total
```

One local test produced:

```text
consumer_dlq_messages_total{source_topic="orders.events",dlq_topic="orders.dlq",reason="invalid-json"} 24
```

Read one DLQ record:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.dlq \
  --from-beginning \
  --max-messages 1
```

The DLQ record should contain `sourceTopic`, `sourcePartition`, `sourceOffset`, the original `value`, `reason`, `error`, and `failedAt`. This context supports later investigation and replay.

### 8. Stop and Clean Up

1. Press `Control+C` in Terminal C to stop the Producer.
2. Press `Control+C` in Terminal B to stop the Consumer.
3. Stop the Docker services from Terminal A:

```bash
docker compose down
```

This removes the containers but preserves the PostgreSQL named volume. Use the following command only when you need to delete all PostgreSQL test data:

```bash
docker compose down -v
```

### 9. Troubleshooting

| Problem | Cause | Resolution |
| --- | --- | --- |
| Kafka or PostgreSQL does not report `healthy` | The service is still starting, or Docker does not have enough resources | Wait and run `docker compose ps` again; inspect `docker compose logs kafka` if needed |
| `orders.events` or `orders.dlq` is missing | Topics have not been created, or the Kafka container was recreated | Run `./scripts/setup-topics.sh` |
| The Consumer has no new output | The Producer is stopped and no new records are entering Kafka | Start the Producer; an idle Consumer is expected to wait quietly |
| Consumer startup reports `EADDRINUSE: 9102` | Another Consumer already owns the metrics port | Find the original Consumer terminal and press `Control+C`; do not run two identical Consumers |
| Producer startup reports `EADDRINUSE: 9101` | Another Producer already owns the metrics port | Find the original Producer terminal and press `Control+C` |
| Lag does not increase | The Producer is not fast enough, or the Consumer delay is disabled | Use `PRODUCER_INTERVAL_MS=50` and `CONSUMER_PROCESSING_DELAY_MS=1000` |
| The Producer does not publish after Kafka recovers | The Producer exhausted its KafkaJS retry budget | Press `Control+C`, then run `npm run start -w apps/producer` again |
| The Consumer does not immediately rejoin after broker recovery | Kafka is not fully healthy yet, and the Consumer is backing off between retries | Wait for `Consumer has joined the group`; restarting the Consumer is normally unnecessary |
| A Kafka CLI command is not found | The Apache Kafka image does not add its CLI directory to the default `PATH` | Use the full `/opt/kafka/bin/kafka-*.sh` path |
| The database event count is unexpectedly large | The PostgreSQL volume contains data from earlier tests | This cumulative result is expected; use `docker compose down -v` to reset it, but note that this deletes data |

## Definitions

| Term | Meaning |
| --- | --- |
| Consumer lag | The difference between the latest Kafka offset and the offset processed by a consumer group. |
| Broker failure | Kafka broker unavailability caused by stopping, restarting, or deleting the broker instance. |
| Duplicate message | More than one Kafka message representing the same logical business event. |
| Poison message | A message that repeatedly fails processing because its payload or business meaning is invalid. |
| Dead-letter topic | A Kafka topic used to store messages that cannot be processed successfully. |
| Idempotency | The ability to process the same logical event more than once without changing the final result incorrectly. |

## Current Status

- Step 1 is complete.
- Step 2 is complete.
- Step 3 is complete.
- Step 4 is complete.
- Step 5 is complete.
- Step 6 is complete.
- Step 7 is complete.
- Step 8 is complete.
- Step 9 is complete.
- Step 10 is complete.
- Step 11 is complete.
- Step 12 is complete.
- Duplicate messages can be simulated and skipped with idempotent database writes.
- Step 13 is complete.
- Poison messages can be routed to `orders.dlq` without blocking the consumer.
- The local pipeline and four scenarios have been exercised end to end.
- Live testing corrections have been committed and verified locally.
- The English step-by-step testing and troubleshooting guide is drafted and awaiting review.
