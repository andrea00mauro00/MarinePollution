CREATE TABLE IF NOT EXISTS anomalies (
  id SERIAL PRIMARY KEY,
  sensor_id TEXT,
  metric TEXT,
  value DOUBLE PRECISION,
  event_time TIMESTAMPTZ DEFAULT now()
);
