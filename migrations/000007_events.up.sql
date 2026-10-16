-- Phase 15 step 1: the ledger's table. One row per sensor per session whose
-- residual move passed the threshold on the close (bearing.md #109; portfolio.md
-- §15 Phase 15). A record, not a feed: a plain table with no retention policy,
-- unlike ticks.
--
-- The row is inserted when the threshold fires. split and coverage start NULL
-- and are filled by the currency-split and news steps; a fetch that fails is
-- written into coverage as an absence, so a trip is never lost to one.
--
-- (symbol, session_date) is the identity: the nightly job is idempotent, and a
-- re-run on the same close cannot write a second row. The country is not
-- stored: it is resolved at read from the reference table.
CREATE TABLE events (
    symbol            TEXT             NOT NULL REFERENCES symbols (symbol),
    session_date      DATE             NOT NULL, -- the New York session
    closed_at         TIMESTAMPTZ      NOT NULL, -- the close news is split at

    -- The trigger, with every input a reader needs to recompute it from
    -- /symbols/{symbol}/candles. threshold is the value in force when it fired.
    symbol_return     DOUBLE PRECISION NOT NULL,
    benchmark         TEXT,                      -- sector ETF; NULL for AAPL
    benchmark_return  DOUBLE PRECISION,
    benchmark_reason  TEXT,                      -- why there is no benchmark
    residual          DOUBLE PRECISION NOT NULL, -- symbol_return - benchmark_return, or symbol_return
    sigma             DOUBLE PRECISION NOT NULL,
    z                 DOUBLE PRECISION NOT NULL,
    threshold         DOUBLE PRECISION NOT NULL,
    window_sessions   INTEGER          NOT NULL,

    -- The currency split. NULL until step 4 runs, and for a trip that has none
    -- (Chile, Colombia, Peru, the global names), where split_label stays NULL and
    -- split carries the reason.
    split_label       TEXT,
    split             JSONB,

    -- The news fetched for the trip, grouped by outlet, with absences. NULL
    -- until step 6 has run.
    coverage          JSONB,
    coverage_fetched_at TIMESTAMPTZ,

    -- Enum list, validated in Go (thin_listing, self_weight, ...).
    caveats           TEXT[]           NOT NULL DEFAULT '{}',

    created_at        TIMESTAMPTZ      NOT NULL DEFAULT now(),

    PRIMARY KEY (symbol, session_date),
    CHECK (split_label IN ('currency-led', 'stock-led', 'both')),
    CHECK (benchmark IS NOT NULL OR (benchmark_return IS NULL AND benchmark_reason IS NOT NULL)),
    CHECK (benchmark IS NULL OR benchmark_return IS NOT NULL),
    CHECK (window_sessions > 0)
);

-- The ledger reads in time order.
CREATE INDEX idx_events_session_date ON events (session_date DESC, symbol);
