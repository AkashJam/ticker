-- Phase 13 step 2: a Finnhub tick is a 1-second bar built from many trades
-- (ADR-004's amendment), so the persisted tape carries the bar's open, high
-- and low alongside price (its close) and volume. Nullable, no default: the
-- sim's single-price ticks leave them NULL, and adding a nullable column
-- without a default is allowed on a compressed hypertable.
ALTER TABLE ticks
    ADD COLUMN open DOUBLE PRECISION,
    ADD COLUMN high DOUBLE PRECISION,
    ADD COLUMN low  DOUBLE PRECISION;
