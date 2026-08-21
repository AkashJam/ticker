// Package store holds the two repository seams portfolio.md §6.3 calls
// for: Timescale (ticks + candles, this file, and meta.go for symbols) and
// Redis (redis.go — lock/pub-sub/cache primitives). Both Timescale
// repositories share one *pgxpool.Pool deliberately kept as separate types
// rather than one big struct, so a future split onto their own instance
// (§6.3's own note) costs nothing beyond swapping which pool each is
// constructed with.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AkashJam/ticker/internal/source"
)

// NewPool opens the shared Timescale/Postgres connection pool used by both
// Timescale and Meta.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return pool, nil
}

// Timescale is the ticks/candles repository (portfolio.md §6.3).
type Timescale struct {
	pool *pgxpool.Pool
}

func NewTimescale(pool *pgxpool.Pool) *Timescale {
	return &Timescale{pool: pool}
}

// InsertTick appends one raw tick — the `ticks` hypertable, 7-day retention
// (ADR-004).
func (t *Timescale) InsertTick(ctx context.Context, tick source.NormalizedTick) error {
	const q = `
		INSERT INTO ticks (time, symbol, price, volume, simulated)
		VALUES ($1, $2, $3, $4, $5)`
	_, err := t.pool.Exec(ctx, q, tick.Timestamp, tick.Symbol, tick.Price, tick.Volume, tick.Simulated)
	if err != nil {
		return fmt.Errorf("store: insert tick: %w", err)
	}
	return nil
}

// UpsertCandle writes the current state of one (symbol, interval, time)
// candle, overwriting whatever was there before. Safe to call repeatedly
// for the same still-open candle as new ticks arrive — the write itself is
// idempotent regardless of how many times it's repeated (ADR-004 / Risk
// Register #4), which is what makes it safe under Redis Streams'
// at-least-once delivery and under the shutdown-flush path.
func (t *Timescale) UpsertCandle(ctx context.Context, c source.Candle) error {
	const q = `
		INSERT INTO candles (time, symbol, interval, open, high, low, close, volume)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (symbol, interval, time) DO UPDATE SET
			open   = EXCLUDED.open,
			high   = EXCLUDED.high,
			low    = EXCLUDED.low,
			close  = EXCLUDED.close,
			volume = EXCLUDED.volume`
	_, err := t.pool.Exec(ctx, q, c.Time, c.Symbol, c.Interval, c.Open, c.High, c.Low, c.Close, c.Volume)
	if err != nil {
		return fmt.Errorf("store: upsert candle: %w", err)
	}
	return nil
}

// defaultCandleLimit bounds an unbounded from/to query — this is reached
// indirectly via a public SSR fetch (§8), so an unbounded result set is a
// real (if minor) resource-exhaustion surface worth defending against by
// default rather than trusting every caller to pass a sane range.
const defaultCandleLimit = 1000

// Candles returns candles for (symbol, interval), optionally bounded by
// [from, to) — a zero time.Time leaves that bound open. Ordered oldest
// first, matching how chart libraries expect series data.
func (t *Timescale) Candles(ctx context.Context, symbol, interval string, from, to time.Time) ([]source.Candle, error) {
	const q = `
		SELECT time, symbol, interval, open, high, low, close, volume
		FROM candles
		WHERE symbol = $1 AND interval = $2
			AND ($3::timestamptz IS NULL OR time >= $3)
			AND ($4::timestamptz IS NULL OR time < $4)
		ORDER BY time ASC
		LIMIT $5`

	var fromArg, toArg *time.Time
	if !from.IsZero() {
		fromArg = &from
	}
	if !to.IsZero() {
		toArg = &to
	}

	rows, err := t.pool.Query(ctx, q, symbol, interval, fromArg, toArg, defaultCandleLimit)
	if err != nil {
		return nil, fmt.Errorf("store: query candles: %w", err)
	}
	defer rows.Close()

	var out []source.Candle
	for rows.Next() {
		var c source.Candle
		if err := rows.Scan(&c.Time, &c.Symbol, &c.Interval, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume); err != nil {
			return nil, fmt.Errorf("store: scan candle: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// LatestCandle returns the single most recent candle for (symbol,
// interval) — used by the snapshot handler's dayHigh/dayLow/prevClose
// fields.
func (t *Timescale) LatestCandle(ctx context.Context, symbol, interval string) (source.Candle, bool, error) {
	const q = `
		SELECT time, symbol, interval, open, high, low, close, volume
		FROM candles
		WHERE symbol = $1 AND interval = $2
		ORDER BY time DESC
		LIMIT 1`
	var c source.Candle
	err := t.pool.QueryRow(ctx, q, symbol, interval).Scan(&c.Time, &c.Symbol, &c.Interval, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return source.Candle{}, false, nil
		}
		return source.Candle{}, false, fmt.Errorf("store: query latest candle: %w", err)
	}
	return c, true, nil
}

// RecentCandles returns up to `limit` of the most recent candles for
// (symbol, interval), oldest first — the shape indicator computation and
// the snapshot handler's day-high/low/prevClose fields both need (as
// opposed to Candles' explicit-range query, which serves the /candles
// endpoint's from/to parameters).
func (t *Timescale) RecentCandles(ctx context.Context, symbol, interval string, limit int) ([]source.Candle, error) {
	const q = `
		SELECT time, symbol, interval, open, high, low, close, volume
		FROM candles
		WHERE symbol = $1 AND interval = $2
		ORDER BY time DESC
		LIMIT $3`
	rows, err := t.pool.Query(ctx, q, symbol, interval, limit)
	if err != nil {
		return nil, fmt.Errorf("store: query recent candles: %w", err)
	}
	defer rows.Close()

	var out []source.Candle
	for rows.Next() {
		var c source.Candle
		if err := rows.Scan(&c.Time, &c.Symbol, &c.Interval, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume); err != nil {
			return nil, fmt.Errorf("store: scan recent candle: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

func (t *Timescale) Ping(ctx context.Context) error {
	return t.pool.Ping(ctx)
}
