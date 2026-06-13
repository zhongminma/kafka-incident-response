#!/usr/bin/env sh
set -eu

POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgres}"
POSTGRES_USER="${POSTGRES_USER:-app}"
POSTGRES_DB="${POSTGRES_DB:-event_stream}"

docker compose exec -T "$POSTGRES_SERVICE" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
\dt
\d consumed_events
SELECT count(*) AS consumed_event_count FROM consumed_events;
SQL
