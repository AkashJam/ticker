-- Fails, by design, while any event carries 'neither': that row would have no
-- valid label to go back to, and deleting the answer is not a rollback.
ALTER TABLE events DROP CONSTRAINT events_split_label_check;
ALTER TABLE events ADD CONSTRAINT events_split_label_check
    CHECK (split_label IN ('currency-led', 'stock-led', 'both'));
