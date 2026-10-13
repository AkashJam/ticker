SELECT remove_compression_policy('ticks', if_exists => true);
SELECT decompress_chunk(c, if_compressed => true) FROM show_chunks('ticks') c;
ALTER TABLE ticks SET (timescaledb.compress = false);
SELECT set_chunk_time_interval('ticks', INTERVAL '7 days');
