-- content/REVIEW.md's central design: the raw-tick insert IS the dedupe.
-- `time` must be part of the unique constraint — a TimescaleDB hypertable
-- requires every unique index to include the partitioning column
-- (`ticks` was hypertable-partitioned on `time` in 000001), so
-- UNIQUE (stream_id) alone is rejected; UNIQUE (time, stream_id) is not,
-- and `time` comes from the tick payload so it's byte-identical on any
-- reprocessing of the same stream entry.
--
-- `ticks` has never been written to in production (InsertTick existed but
-- nothing called it), so no backfill is needed for the NOT NULL column.
ALTER TABLE ticks ADD COLUMN stream_id TEXT NOT NULL;
ALTER TABLE ticks ADD CONSTRAINT ticks_time_stream_id_key UNIQUE (time, stream_id);
