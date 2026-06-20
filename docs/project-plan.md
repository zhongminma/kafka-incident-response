# Project Plan and Status

This document tracks the delivery plan, contribution approval rules, development strategy, and current implementation status for the Kafka Incident Response project.

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

## Current Status

- Steps 1 through 13 are complete.
- Duplicate messages can be simulated and skipped with idempotent database writes.
- Poison messages can be routed to `orders.dlq` without blocking the consumer.
- The local pipeline and all four incident scenarios have been exercised end to end.
- Live testing corrections have been committed and verified locally.
- The English step-by-step testing and troubleshooting guide is complete.
- Step 14, OpenTelemetry tracing, is the next planned delivery step.

## Verification History

| Step | Verification |
| --- | --- |
| 2 | Workspace skeleton and package metadata inspected with `git status` and `find`. |
| 3 | `docker compose config` resolved Kafka and PostgreSQL configuration. |
| 4 | `scripts/setup-topics.sh` passed shell syntax validation and created both topics idempotently. |
| 5 | Producer syntax passed and valid `order.created` events were published. |
| 6 | Consumer syntax passed and valid events were stored in PostgreSQL. |
| 7 | Database summary script passed syntax validation and inspected persisted events. |
| 8 | Control API syntax passed; health and scenario endpoints were verified. |
| 9 | Artificial producer rate and consumer delay reproduced increasing lag. |
| 10 | Producer and Consumer metrics endpoints exposed Prometheus metrics. |
| 11 | Broker stop and restart workflow verified failure detection and Consumer recovery. |
| 12 | Duplicate publishing produced `Skipped duplicate` logs and idempotent database writes. |
| 13 | Invalid records were routed to `orders.dlq` without blocking valid events. |
| Integration checkpoint | All four scenarios were exercised locally; detailed results are in `docs/end-to-end-testing.md`. |
