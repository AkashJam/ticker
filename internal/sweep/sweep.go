package sweep

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/AkashJam/ticker/internal/source"
)

// The sweep fires at fireHour:fireMinute New York time. The regular session
// closes at 16:00 (13:00 on a half-day), so by this time a quote's last
// trade is that session's closing print.
const (
	fireHour   = 16
	fireMinute = 15
	// DailyInterval is the candles.interval the sweep writes.
	DailyInterval = "1d"
)

// Store is what the runner needs from Timescale.
type Store interface {
	UpsertCandle(ctx context.Context, c source.Candle) error
	HasCandle(ctx context.Context, symbol, interval string, at time.Time) (bool, error)
}

// Mode picks how Run decides whether there is anything to do.
type Mode int

const (
	// ModeScheduled is the 16:15 fire: the session must be today's. An older
	// session date means the exchange was closed (a holiday): nothing to write.
	ModeScheduled Mode = iota
	// ModeCatchUp is the start-up check: sweep the last completed session,
	// but only if the canary has no bar for it yet.
	ModeCatchUp
	// ModeManual is `ticker sweep`: sweep the last completed session whether
	// or not a bar exists. Safe to repeat — the upsert is idempotent.
	ModeManual
)

// State is the outcome of a Run.
type State string

const (
	StateWritten  State = "written"    // every symbol written
	StateFailed   State = "failed"     // at least one symbol not written
	StateHoliday  State = "holiday"    // no session today: nothing to write, correctly
	StateNotFinal State = "not-final"  // the quote's session is still open
	StateUpToDate State = "up-to-date" // catch-up found the canary's bar already there
)

// Result reports a Run.
type Result struct {
	State   State
	Session time.Time // the session date, midnight UTC; zero when none was found
	Written int
	Failed  []string
}

// Runner sweeps Symbols once. The first symbol is the canary: its quote
// decides which session this is and whether it is complete, so a holiday or a
// still-open market costs one call, not 28.
type Runner struct {
	Quoter   Quoter
	Store    Store
	Symbols  []string
	Loc      *time.Location
	Gap      time.Duration // between symbols; Finnhub's free tier allows 60/min and 30/s
	Attempts int           // per symbol
	Backoff  time.Duration // before a retry, when the server names no wait
	Now      func() time.Time
	Sleep    func(ctx context.Context, d time.Duration) error
	Log      *slog.Logger
}

// NewRunner returns a Runner with the production pacing: one call a second,
// three attempts a symbol.
func NewRunner(q Quoter, st Store, symbols []string, loc *time.Location, log *slog.Logger) *Runner {
	return &Runner{
		Quoter: q, Store: st, Symbols: symbols, Loc: loc,
		Gap: time.Second, Attempts: 3, Backoff: 3 * time.Second,
		Now: time.Now, Sleep: sleepCtx, Log: log,
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Run executes one sweep. A non-nil error means the run could not even start
// (no symbols, or the context ended); per-symbol failures are in the Result.
func (r *Runner) Run(ctx context.Context, mode Mode) (Result, error) {
	if len(r.Symbols) == 0 {
		return Result{}, errors.New("sweep: no symbols")
	}
	canary := r.Symbols[0]
	q, err := r.quote(ctx, canary)
	if err != nil {
		if ctx.Err() != nil {
			return Result{}, ctx.Err()
		}
		r.Log.Error("sweep: canary quote failed", "symbol", canary, "error", err)
		return Result{State: StateFailed, Failed: []string{canary}}, nil
	}

	session := r.sessionDate(q)
	now := r.Now().In(r.Loc)
	res := Result{Session: session}

	if now.Before(r.finalAt(session)) {
		res.State = StateNotFinal
		return res, nil
	}
	if mode == ModeScheduled && session.Before(r.todayUTC(now)) {
		res.State = StateHoliday
		return res, nil
	}
	if mode == ModeCatchUp {
		has, err := r.Store.HasCandle(ctx, canary, DailyInterval, session)
		if err != nil {
			return Result{}, fmt.Errorf("sweep: catch-up check: %w", err)
		}
		if has {
			res.State = StateUpToDate
			return res, nil
		}
	}

	for i, symbol := range r.Symbols {
		if i > 0 {
			if err := r.Sleep(ctx, r.Gap); err != nil {
				return res, err
			}
			q, err = r.quote(ctx, symbol)
		}
		if err == nil {
			err = r.write(ctx, symbol, session, q)
		}
		if err != nil {
			if ctx.Err() != nil {
				return res, ctx.Err()
			}
			r.Log.Error("sweep: symbol failed", "symbol", symbol, "error", err)
			res.Failed = append(res.Failed, symbol)
			err = nil
			continue
		}
		res.Written++
	}

	res.State = StateWritten
	if len(res.Failed) > 0 {
		res.State = StateFailed
	}
	return res, nil
}

// quote fetches one symbol, retrying what can succeed on a retry.
func (r *Runner) quote(ctx context.Context, symbol string) (Quote, error) {
	var lastErr error
	for attempt := 1; attempt <= r.Attempts; attempt++ {
		q, err := r.Quoter.Quote(ctx, symbol)
		if err == nil {
			return q, nil
		}
		lastErr = err
		if ctx.Err() != nil || attempt == r.Attempts {
			break
		}
		var se *StatusError
		wait := r.Backoff
		switch {
		case errors.As(err, &se):
			if !se.Retryable() {
				return Quote{}, err
			}
			if se.RetryAfter > wait {
				wait = se.RetryAfter
			}
		case errors.Is(err, ErrNoData):
			return Quote{}, err // retrying an unknown symbol only wastes budget
		}
		if err := r.Sleep(ctx, wait); err != nil {
			return Quote{}, err
		}
	}
	return Quote{}, lastErr
}

// write stores one symbol's bar for session, after checking that the quote
// really belongs to that session.
func (r *Runner) write(ctx context.Context, symbol string, session time.Time, q Quote) error {
	if got := r.sessionDate(q); !got.Equal(session) {
		return fmt.Errorf("stale quote: session %s, want %s", got.Format(time.DateOnly), session.Format(time.DateOnly))
	}
	if q.Open <= 0 || q.High <= 0 || q.Low <= 0 || q.Current <= 0 || q.High < q.Low {
		return fmt.Errorf("invalid quote: o=%v h=%v l=%v c=%v", q.Open, q.High, q.Low, q.Current)
	}
	// Volume stays nil: /quote has none, and absent is honest where 0 is not.
	return r.Store.UpsertCandle(ctx, source.Candle{
		Time: session, Symbol: symbol, Interval: DailyInterval,
		Open: q.Open, High: q.High, Low: q.Low, Close: q.Current,
	})
}

// sessionDate is the quote's New York calendar date as midnight UTC — the
// same grid the aggregator's 1d buckets use (Truncate(24h)). It comes from
// the quote's own timestamp, not the fetch time: 28 staggered responses
// carry 28 wall-clock times for one close.
func (r *Runner) sessionDate(q Quote) time.Time {
	y, m, d := q.At.In(r.Loc).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func (r *Runner) todayUTC(now time.Time) time.Time {
	y, m, d := now.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// finalAt is when a session's last trade is certain to be its close.
func (r *Runner) finalAt(session time.Time) time.Time {
	y, m, d := session.Date()
	return time.Date(y, m, d, fireHour, fireMinute, 0, 0, r.Loc)
}
