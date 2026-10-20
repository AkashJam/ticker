-- Phase 17 step 4, decision 7: GET /symbols, the only reader of `tracked`, is
-- removed. Phase 14 never flipped the flag (GET /sensors lists the sensors), so
-- the column only recorded which rows a deleted endpoint would have listed.
ALTER TABLE symbols DROP COLUMN tracked;
