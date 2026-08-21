-- portfolio.md §6.3 — ticks/candles hypertables + symbols metadata.
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE ticks (
    time      TIMESTAMPTZ NOT NULL,
    symbol    TEXT NOT NULL,
    price     DOUBLE PRECISION NOT NULL,
    volume    DOUBLE PRECISION,
    simulated BOOLEAN NOT NULL DEFAULT false
);
SELECT create_hypertable('ticks', 'time');
SELECT add_retention_policy('ticks', INTERVAL '7 days');
CREATE INDEX idx_ticks_symbol_time ON ticks (symbol, time DESC);

CREATE TABLE candles (
    time     TIMESTAMPTZ NOT NULL,
    symbol   TEXT NOT NULL,
    interval TEXT NOT NULL,
    open     DOUBLE PRECISION,
    high     DOUBLE PRECISION,
    low      DOUBLE PRECISION,
    close    DOUBLE PRECISION,
    volume   DOUBLE PRECISION,
    PRIMARY KEY (symbol, interval, time)
);
SELECT create_hypertable('candles', 'time');

CREATE TABLE symbols (
    symbol   TEXT PRIMARY KEY,
    name     TEXT NOT NULL,
    type     TEXT NOT NULL,
    exchange TEXT,
    tracked  BOOLEAN NOT NULL DEFAULT true
);

-- Seed the v1 SIM: universe (§6.2) — the Cost-of-Living family isn't
-- seeded here since §6.3 serves it computed from the sim source, not a
-- table.
INSERT INTO symbols (symbol, name, type, exchange) VALUES
    ('SIM:NOVA',  'Nova (simulated)',  'stock', 'SIM'),
    ('SIM:HELIX', 'Helix (simulated)', 'stock', 'SIM'),
    ('SIM:ORBIT', 'Orbit (simulated)', 'stock', 'SIM'),
    ('SIM:PULSE', 'Pulse (simulated)', 'stock', 'SIM');
