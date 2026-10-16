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

// Event returns the recorded trip for (symbol, session), if any.
func (t *Timescale) Event(ctx context.Context, symbol string, session time.Time) (trigger.Trip, bool, error) {
	const q = `
		SELECT symbol, session_date, closed_at,
			symbol_return, benchmark, benchmark_return, benchmark_reason,
			residual, sigma, z, threshold, window_sessions, caveats
		FROM events
		WHERE symbol = $1 AND session_date = $2`
	var e trigger.Trip
	err := t.pool.QueryRow(ctx, q, symbol, session).Scan(
		&e.Symbol, &e.SessionDate, &e.ClosedAt,
		&e.SymbolReturn, &e.Benchmark, &e.BenchmarkReturn, &e.BenchmarkReason,
		&e.Residual, &e.Sigma, &e.Z, &e.Threshold, &e.WindowSessions, &e.Caveats)
	if errors.Is(err, pgx.ErrNoRows) {
		return trigger.Trip{}, false, nil
	}
	if err != nil {
		return trigger.Trip{}, false, fmt.Errorf("store: read event: %w", err)
	}
	return e, true, nil
}
