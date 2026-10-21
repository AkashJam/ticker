// Package fixture records the e2e fixture's events (phase17.md step 5): the
// populated ledger the portfolio's fixture suite and contract test read from a
// real ticker. It runs the production judge (trigger.Runner) on the closes
// migration 000006 backfills, at a threshold low enough that every judged sensor
// trips, and keeps only the sensors the story picks. The returns, sigma, z, band
// and currency split are therefore the production arithmetic on real data; the
// split's rates are a committed snapshot of the real ECB window. Only what the
// network would have returned to the news collector is invented (story.go), and
// it goes through the production collector, so the stored coverage is shaped by
// the same code as in production.
//
// It is for dev and CI. It refuses ENV=prod, and refuses any database that holds
// a daily bar after the backfill, which only a real sweep writes.
package fixture

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"slices"
	"time"

	"github.com/AkashJam/ticker/internal/coverage"
	"github.com/AkashJam/ticker/internal/ecb"
	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/store"
	"github.com/AkashJam/ticker/internal/trigger"
)

// BackfillEnd is the last session migration 000006 writes. A database with a
// daily bar after it has seen a real sweep, and is not a fixture database.
var BackfillEnd = time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)

// Threshold is the |z| the fixture judges at: low enough that every sensor with
// sixty returns of history fires (Fire is |z| > threshold, and the flag must be
// above zero), so which sensors appear is the story's choice, not the data's.
// Every event stores it, so the page says what each was judged against.
const Threshold = 0.001

// Pending is the sensor recorded with its split and coverage still pending: the
// fixture's rates and collector both decline it.
const Pending = "RY"

// The states the portfolio's fixture suite runs (portfolio/e2e-fixture/ports.ts).
const (
	StateTripped = "tripped" // the newest backfilled session tripped
	StateQuiet   = "quiet"   // the newest trip is two sessions old
)

// pick is one judged session: back is how many sessions before the newest
// backfilled one, and symbols the sensors kept from it.
type pick struct {
	back    int
	symbols []string
}

// Plans are judged oldest first, so each run also takes the follow-up fetch for
// the earlier trips, as the scheduled run does. Sigma needs 61 closes, and the
// backfill starts on 1 July, so only the last eight or nine sessions can be
// judged; the oldest pick is eight back.
//
//	CIB  Colombia: no atlas row, no ECB currency, a central bank feed that keeps failing
//	CAT  a global name: no split, an 8-K with item codes
//	ITUB Brazil: a Portuguese central bank release, a dividend source that errors
//	KB   Korea: a thin listing, an ex-dividend date on the session
//	MUFG Japan: the lead, every source answering, the record page
//	RY   Canada: split and coverage still pending
//
// NVDA is never picked: the record spec needs a global name with no events.
var plans = map[string][]pick{
	StateTripped: {
		{back: 8, symbols: []string{"CIB"}},
		{back: 6, symbols: []string{"CAT"}},
		{back: 4, symbols: []string{"ITUB"}},
		{back: 2, symbols: []string{"KB"}},
		{back: 0, symbols: []string{"MUFG", Pending}},
	},
	StateQuiet: {
		{back: 8, symbols: []string{"CIB"}},
		{back: 6, symbols: []string{"CAT"}},
		{back: 4, symbols: []string{"ITUB"}},
		{back: 2, symbols: []string{"KB"}},
	},
}

// Store is what the fixture needs from Timescale.
type Store interface {
	trigger.RangeReader
	trigger.EventStore
	LatestCandle(ctx context.Context, symbol, interval string) (source.Candle, bool, error)
}

// Deps is what Run needs.
type Deps struct {
	Store          Store
	Thin           trigger.ThinFunc
	Loc            *time.Location // America/New_York
	Env            string
	SplitThreshold float64
	Log            *slog.Logger
}

//go:embed all:data
var data embed.FS

// stateFS holds StateFile; the tests swap in a synthetic window.
var stateFS fs.FS = data

// StateFile is the ECB window snapshot, written by `ticker ecb --state-out`.
const StateFile = "data/ecb_state.json"

// Run records the state's events and returns each judged session's summary.
func Run(ctx context.Context, d Deps, state string) ([]trigger.Summary, error) {
	plan, ok := plans[state]
	if !ok {
		return nil, fmt.Errorf("fixture: unknown state %q (want %s or %s)", state, StateTripped, StateQuiet)
	}
	if err := guard(ctx, d.Env, d.Store); err != nil {
		return nil, err
	}
	sessions, err := backfillSessions(ctx, d.Store)
	if err != nil {
		return nil, err
	}
	ref, err := referenceWithRates()
	if err != nil {
		return nil, err
	}
	trips := map[string]time.Time{}
	for _, p := range plan {
		if p.back >= len(sessions) {
			return nil, fmt.Errorf("fixture: the plan reaches %d sessions back, and the backfill has %d", p.back, len(sessions))
		}
		for _, s := range p.symbols {
			trips[s] = sessions[len(sessions)-1-p.back]
		}
	}
	st, err := newStory(ctx, coverage.ReferenceConfig{Store: ref}, trips, d.Loc)
	if err != nil {
		return nil, err
	}

	clk := &clock{}
	collector := coverage.NewCollector(coverage.ReferenceConfig{Store: ref}, st, st, st, d.Log)
	collector.Dividends = st
	collector.Now = clk.now
	collector.Sleep = func(context.Context, time.Duration) error { return nil }
	collector.NewsGap = 0

	var out []trigger.Summary
	for _, p := range plan {
		session := sessions[len(sessions)-1-p.back]
		// The coverage is stamped half an hour after the session's close, as the
		// scheduled run (16:15 New York) would stamp it.
		clk.set(time.Date(session.Year(), session.Month(), session.Day(), 16, 30, 0, 0, d.Loc))
		runner := &trigger.Runner{
			Candles:        only{inner: d.Store, keep: p.symbols},
			Events:         followUps{EventStore: d.Store},
			Thin:           d.Thin,
			Loc:            d.Loc,
			Threshold:      Threshold,
			Rates:          rates{inner: &ecb.RateSource{Store: ref, Feed: noNetwork{}}},
			SplitThreshold: d.SplitThreshold,
			Coverage:       collect{inner: collector},
			Log:            d.Log,
		}
		sum, err := runner.Run(ctx, session)
		if err != nil {
			return out, fmt.Errorf("fixture: judge %s: %w", session.Format(time.DateOnly), err)
		}
		if err := check(sum, p.symbols); err != nil {
			return out, err
		}
		out = append(out, sum)
	}
	return out, nil
}

// guard refuses production, by its flag and by its data.
func guard(ctx context.Context, env string, st Store) error {
	if env == "prod" {
		return errors.New("fixture: refused with ENV=prod: it records invented news as events")
	}
	canary := source.SweepUniverse[0].Symbol
	latest, ok, err := st.LatestCandle(ctx, canary, trigger.DailyInterval)
	if err != nil {
		return fmt.Errorf("fixture: %w", err)
	}
	if !ok {
		return fmt.Errorf("fixture: no daily bars for %s; run `ticker migrate` first", canary)
	}
	if latest.Time.UTC().After(BackfillEnd) {
		return fmt.Errorf("fixture: refused: %s has a daily bar on %s, after the backfill ends (%s), so a real sweep has run on this database",
			canary, latest.Time.UTC().Format(time.DateOnly), BackfillEnd.Format(time.DateOnly))
	}
	return nil
}

// backfillSessions is the canary's sessions, oldest first.
func backfillSessions(ctx context.Context, st Store) ([]time.Time, error) {
	bars, err := st.Candles(ctx, source.SweepUniverse[0].Symbol, trigger.DailyInterval, time.Time{}, BackfillEnd.AddDate(0, 0, 1))
	if err != nil {
		return nil, fmt.Errorf("fixture: sessions: %w", err)
	}
	out := make([]time.Time, 0, len(bars))
	for _, b := range bars {
		out = append(out, b.Time.UTC().Truncate(24*time.Hour))
	}
	return out, nil
}

// check fails the run when a picked sensor did not become an event: too little
// history, a missing close, or a z of exactly zero.
func check(sum trigger.Summary, picked []string) error {
	for _, s := range picked {
		if reason, skipped := sum.Skipped[s]; skipped {
			return fmt.Errorf("fixture: %s on %s was not evaluated: %s", s, sum.Session.Format(time.DateOnly), reason)
		}
		if !slices.ContainsFunc(sum.Trips, func(t trigger.Trip) bool { return t.Symbol == s }) {
			return fmt.Errorf("fixture: %s on %s did not trip at %g sigma", s, sum.Session.Format(time.DateOnly), Threshold)
		}
	}
	return nil
}

// referenceWithRates is the embedded reference tables with the ECB window
// snapshot in front of them, as the cache would hold it in production.
func referenceWithRates() (*reference.Store, error) {
	b, err := fs.ReadFile(stateFS, StateFile)
	if err != nil {
		return nil, fmt.Errorf("fixture: %s is missing; write it with `ticker ecb --state-out internal/fixture/%s`: %w", StateFile, StateFile, err)
	}
	cache := memCache{reference.CacheKey(ecb.StateName): string(b)}
	return reference.New(cache, reference.Embedded(), slog.New(slog.DiscardHandler)), nil
}

// only hides the closes of every sensor the run does not keep, so the judge
// evaluates the kept ones alone. Benchmarks are sector funds, never sensors, so
// they stay; and the calendar is every symbol's dates, so it is unchanged.
type only struct {
	inner trigger.RangeReader
	keep  []string
}

func (o only) Candles(ctx context.Context, symbol, interval string, from, to time.Time) ([]source.Candle, error) {
	if isSensor(symbol) && !slices.Contains(o.keep, symbol) {
		return nil, nil
	}
	return o.inner.Candles(ctx, symbol, interval, from, to)
}

func isSensor(symbol string) bool {
	return slices.ContainsFunc(source.StreamUniverse(), func(s source.SymbolInfo) bool { return s.Symbol == symbol })
}

// followUps lets every run take the follow-up fetch. The scheduled run waits
// RetryGap after a fetch before the next, measured against the database's
// clock; the fixture's runs are seconds apart, so it forgets when each was last
// fetched.
type followUps struct{ trigger.EventStore }

func (f followUps) PendingCoverage(ctx context.Context, before time.Time) ([]trigger.Trip, error) {
	evs, err := f.EventStore.PendingCoverage(ctx, before)
	for i := range evs {
		evs[i].CoverageFetchedAt = nil
	}
	return evs, err
}

// rates declines the pending sensor with a failure worth retrying, which leaves
// its split pending, and passes every other sensor to the real rate source.
type rates struct{ inner trigger.RateSource }

func (r rates) Rates(ctx context.Context, symbol string, session time.Time) (trigger.RateSet, error) {
	if symbol == Pending {
		return trigger.RateSet{}, errors.New("fixture: this sensor's split is left pending")
	}
	return r.inner.Rates(ctx, symbol, session)
}

// collect declines the pending sensor, which leaves its coverage unwritten.
type collect struct{ inner trigger.CoverageSource }

func (c collect) Collect(ctx context.Context, ev trigger.Trip, prev []byte, stage string) ([]byte, error) {
	if ev.Symbol == Pending {
		return nil, errors.New("fixture: this sensor's coverage is left pending")
	}
	return c.inner.Collect(ctx, ev, prev, stage)
}

// noNetwork is the ECB feed: the snapshot covers every judged session, so a
// call means a gap, and it fails rather than reaching the network.
type noNetwork struct{}

func (noNetwork) History(context.Context) ([]ecb.Day, error) {
	return nil, errors.New("fixture: the ECB snapshot does not cover this session")
}

func (noNetwork) Daily(context.Context) (ecb.Day, error) {
	return ecb.Day{}, errors.New("fixture: the ECB snapshot does not cover this session")
}

// memCache is the reference cache, in memory.
type memCache map[string]string

func (m memCache) CacheGet(_ context.Context, key string) (string, error) {
	v, ok := m[key]
	if !ok {
		return "", store.ErrCacheMiss
	}
	return v, nil
}

func (m memCache) CacheSet(_ context.Context, key, value string, _ time.Duration) error {
	m[key] = value
	return nil
}

// clock is the collector's clock, set to each run's fetch time.
type clock struct{ t time.Time }

func (c *clock) set(t time.Time) { c.t = t }
func (c *clock) now() time.Time  { return c.t }
