-- Executar dentro do DB 'databridge'
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE SCHEMA IF NOT EXISTS ingest AUTHORIZATION databridge;

CREATE TABLE IF NOT EXISTS ingest.readings (
  id                BIGSERIAL PRIMARY KEY,
  application_name  TEXT,
  device_profile    TEXT NOT NULL,
  device_name       TEXT NOT NULL,
  ts                TIMESTAMPTZ NOT NULL,
  payload           JSONB NOT NULL
);

SELECT create_hypertable('ingest.readings','ts',
                         chunk_time_interval => INTERVAL '7 days',
                         if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_readings_dev_ts
  ON ingest.readings (device_profile, device_name, ts DESC);

CREATE INDEX IF NOT EXISTS idx_readings_ts
  ON ingest.readings (ts DESC);

CREATE INDEX IF NOT EXISTS idx_readings_payload_gin
  ON ingest.readings USING GIN (payload jsonb_path_ops);

ALTER TABLE ingest.readings
  SET (timescaledb.compress,
       timescaledb.compress_orderby = 'ts DESC',
       timescaledb.compress_segmentby = 'device_profile, device_name');

SELECT add_compression_policy('ingest.readings', INTERVAL '30 days');
SELECT add_retention_policy('ingest.readings', INTERVAL '365 days');
