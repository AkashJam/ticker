-- Phase 15 step 4: a trip can fire (the residual passed 2.5 sigma) while neither
-- the currency part nor the stock part passes 2 sigma on its own, a case
-- bearing.md #90 does not cover. 'neither' says so, and NULL stays reserved for
-- "no split": pending, or no split with a reason in the split JSON.
ALTER TABLE events DROP CONSTRAINT events_split_label_check;
ALTER TABLE events ADD CONSTRAINT events_split_label_check
    CHECK (split_label IN ('currency-led', 'stock-led', 'both', 'neither'));
