package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/AkashJam/ticker/internal/trigger"
)

// InsertEvent records a trip in the events table (migration 000007). The
// ledger is a record: an event already recorded for (symbol, session_date)
// stays exactly as it was, and inserted reports whether this call wrote it.
func (t *Timescale) InsertEvent(ctx context.Context, e trigger.Trip) (inserted bool, err error) {
	const q = `
		INSERT INTO events (
			symbol, session_date, closed_at,
			symbol_return, benchmark, benchmark_return, benchmark_reason,
			residual, sigma, z, threshold, window_sessions, caveats
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (symbol, session_date) DO NOTHING`
	caveats := e.Caveats
	if caveats == nil {
		caveats = []string{}
	}
	tag, err := t.pool.Exec(ctx, q,
		e.Symbol, e.SessionDate, e.ClosedAt,
		e.SymbolReturn, e.Benchmark, e.BenchmarkReturn, e.BenchmarkReason,
		e.Residual, e.Sigma, e.Z, e.Threshold, e.WindowSessions, caveats)
	if err != nil {
		return false, fmt.Errorf("store: insert event: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

const eventColumns = `
	symbol, session_date, closed_at,
	symbol_return, benchmark, benchmark_return, benchmark_reason,
	residual, sigma, z, threshold, window_sessions, caveats,
	split_label, split, coverage, coverage_fetched_at`

func scanTrip(row pgx.Row) (trigger.Trip, error) {
	var e trigger.Trip
	err := row.Scan(
		&e.Symbol, &e.SessionDate, &e.ClosedAt,
		&e.SymbolReturn, &e.Benchmark, &e.BenchmarkReturn, &e.BenchmarkReason,
		&e.Residual, &e.Sigma, &e.Z, &e.Threshold, &e.WindowSessions, &e.Caveats,
		&e.SplitLabel, &e.Split, &e.Coverage, &e.CoverageFetchedAt)
	return e, err
}

// Event returns the recorded trip for (symbol, session), if any.
func (t *Timescale) Event(ctx context.Context, symbol string, session time.Time) (trigger.Trip, bool, error) {
	e, err := scanTrip(t.pool.QueryRow(ctx, `SELECT `+eventColumns+` FROM events WHERE symbol = $1 AND session_date = $2`, symbol, session))
	if errors.Is(err, pgx.ErrNoRows) {
		return trigger.Trip{}, false, nil
	}
	if err != nil {
		return trigger.Trip{}, false, fmt.Errorf("store: read event: %w", err)
	}
	return e, true, nil
}

// pendingWindowDays is how far back PendingCoverage looks: a source that has not
// answered in a fortnight is not going to.
const pendingWindowDays = 14

// PendingCoverage returns the events of the last fortnight before session whose
// coverage is missing or not yet complete, oldest first.
func (t *Timescale) PendingCoverage(ctx context.Context, before time.Time) ([]trigger.Trip, error) {
	rows, err := t.pool.Query(ctx, `SELECT `+eventColumns+` FROM events
		WHERE session_date < $1 AND session_date >= $1 - $2::int
			AND (coverage IS NULL OR (coverage->>'complete') IS DISTINCT FROM 'true')
		ORDER BY session_date, symbol`, before, pendingWindowDays)
	if err != nil {
		return nil, fmt.Errorf("store: pending coverage: %w", err)
	}
	defer rows.Close()
	var out []trigger.Trip
	for rows.Next() {
		e, err := scanTrip(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan pending coverage: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// SetCoverage records an event's coverage, replacing the earlier stage's.
func (t *Timescale) SetCoverage(ctx context.Context, symbol string, session time.Time, coverage []byte) error {
	tag, err := t.pool.Exec(ctx, `UPDATE events SET coverage = $3::jsonb, coverage_fetched_at = now()
		WHERE symbol = $1 AND session_date = $2`, symbol, session, string(coverage))
	if err != nil {
		return fmt.Errorf("store: set coverage: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("store: set coverage: no event for %s on %s", symbol, session.Format(time.DateOnly))
	}
	return nil
}

// SetSplit records an event's currency split (phase15.md step 4). It writes
// only where the event has none, so a split, once made, is never rewritten, and
// set reports whether this call wrote it. label is NULL for a "no split" answer.
func (t *Timescale) SetSplit(ctx context.Context, symbol string, session time.Time, label *string, split []byte) (set bool, err error) {
	const q = `
		UPDATE events SET split_label = $3, split = $4::jsonb
		WHERE symbol = $1 AND session_date = $2 AND split IS NULL`
	tag, err := t.pool.Exec(ctx, q, symbol, session, label, string(split))
	if err != nil {
		return false, fmt.Errorf("store: set split: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
