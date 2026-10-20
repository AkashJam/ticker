-- Phase 17 step 4, decision 8: nothing has read ticks.simulated since Phase 14
-- (ADR-006), and every production row since 16 October is false. The flag
-- stays on NormalizedTick, inside the pipeline: the aggregator still uses it
-- to give SIM: symbols their own daily bar. Dropping a column that is neither
-- segmentby (symbol) nor orderby (time) is supported on a compressed
-- hypertable since TimescaleDB 2.10; the deploy runs 2.20.3.
ALTER TABLE ticks DROP COLUMN simulated;
