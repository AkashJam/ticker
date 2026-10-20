-- Restores the values, not only the column: the flag was set by the simulator
-- and only the simulator emits SIM: symbols, so the prefix gives it back
-- exactly. The UPDATE decompresses any compressed chunk it touches; it runs
-- only on a rollback.
ALTER TABLE ticks ADD COLUMN simulated BOOLEAN NOT NULL DEFAULT false;
UPDATE ticks SET simulated = true WHERE symbol LIKE 'SIM:%';
