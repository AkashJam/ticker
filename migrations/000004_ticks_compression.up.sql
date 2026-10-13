-- Phase 13 step 1: compress `ticks` (portfolio.md §15). Retention has been 7
-- days since 000001 but nothing compressed, so a real feed would hold ~1.9 GB
-- of ticks; compressed it is ~440 MB.
--
-- The chunk interval drops from the 7-day default to 1 day first. A chunk is
-- compressed only once the whole chunk is older than compress_after, so with
-- 7-day chunks and 7-day retention almost nothing would ever be compressed
-- before it was dropped. The interval applies to new chunks only; the chunk
-- that exists today ages out under retention.
--
-- segmentby symbol keeps one symbol's ticks together (every read filters by
-- symbol). The (time, stream_id) unique constraint from 000002 still dedupes
-- an insert into a compressed chunk (supported since TimescaleDB 2.11; the
-- deploy runs 2.20.3).
SELECT set_chunk_time_interval('ticks', INTERVAL '1 day');
ALTER TABLE ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('ticks', INTERVAL '1 day');
