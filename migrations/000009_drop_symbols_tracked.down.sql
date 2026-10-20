-- Restores the values the column held, not only its shape: 000003's down
-- deletes the sweep rows by `tracked = false`, so they must read false again.
ALTER TABLE symbols ADD COLUMN tracked BOOLEAN NOT NULL DEFAULT true;
UPDATE symbols SET tracked = false WHERE symbol NOT LIKE 'SIM:%';
