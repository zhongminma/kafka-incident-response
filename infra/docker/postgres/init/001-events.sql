CREATE TABLE IF NOT EXISTS consumed_events (
  id BIGSERIAL PRIMARY KEY,
  event_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  order_id UUID NOT NULL,
  topic TEXT NOT NULL,
  partition_id INTEGER NOT NULL,
  message_offset BIGINT NOT NULL,
  payload JSONB NOT NULL,
  consumed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS consumed_events_event_type_idx
  ON consumed_events (event_type);

CREATE INDEX IF NOT EXISTS consumed_events_order_id_idx
  ON consumed_events (order_id);

CREATE INDEX IF NOT EXISTS consumed_events_consumed_at_idx
  ON consumed_events (consumed_at);
