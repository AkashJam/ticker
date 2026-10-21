`ecb_state.json` is the ECB daily-rate window the fixture's currency split reads
(phase17.md step 5). It is real data, written once with network access:

    go run ./cmd ecb --state-out internal/fixture/data/ecb_state.json

It must cover every session the fixture judges (the last nine before
8 October 2026), so `last` must be on or after 2026-10-08. Regenerate it only
if the fixture's sessions move.
