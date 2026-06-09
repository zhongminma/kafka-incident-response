# Local Dependencies

Step 3 provides the local dependency layer for the Kafka incident response lab.

## Services

| Service | Image | Local Port | Purpose |
| --- | --- | --- | --- |
| Kafka | `bitnami/kafka:3.7` | `9092` | Local single-node Kafka broker using KRaft mode. |
| PostgreSQL | `postgres:16-alpine` | `5432` | Local database for consumed events. |

## Start

```bash
docker compose up -d
```

## Check Status

```bash
docker compose ps
```

Kafka is ready when the `kafka` service health check passes. PostgreSQL is ready when `pg_isready` passes.

## Stop

```bash
docker compose down
```

## Remove Local Data

```bash
docker compose down -v
```

This removes the local Kafka and PostgreSQL volumes. Use it only when a clean dependency state is needed.

## Step 3 Verification

```bash
docker compose config
```

This verifies that the Compose files are syntactically valid and can be resolved from the repository root.
