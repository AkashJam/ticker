// Package trigger computes the quantities Phase 15's threshold is made of: a
// sensor's daily residual return against its sector benchmark, the standard
// deviation of the sixty residuals before a day, and the day's z (portfolio.md
// §15 Phase 15; phase15.md step 2).
//
// Everything is a pure function of daily closes, recomputed on demand. Nothing
// is stored, so a reader can reproduce any figure from /symbols/{symbol}/candles.
//
// Conventions (agreed in phase15.md step 2): simple close-to-close returns; the
// residual is symbol minus benchmark; sigma is the sample standard deviation
// (n-1) about the window's mean; the window is the sixty most recent valid
// residuals strictly before the tested day, so a move never inflates its own
// yardstick; and a gap is never bridged.
package trigger

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/AkashJam/ticker/internal/source"
)

// Window is the number of residual returns sigma is taken over.
const Window = 60

// DailyInterval is the candles.interval read.
const DailyInterval = "1d"

// loadLimit is how many recent daily bars Load reads per symbol: the window,
// the tested day, the close before it, and room for a few missing days.
const loadLimit = Window + 30

var (
	// ErrNoReturn means the tested day has no valid return: the symbol or its
	// benchmark lacks a bar on that day or on the session before.
	ErrNoReturn = errors.New("trigger: no valid return on the tested day")
	// ErrNoSession means no symbol has a bar for the requested session.
	ErrNoSession = errors.New("trigger: no symbol has a bar for the session")
	// ErrZeroSigma means the window was flat, so z is undefined.
	ErrZeroSigma = errors.New("trigger: sigma is zero")
)

// InsufficientHistoryError means fewer than Window valid residuals precede the
// tested day. The symbol cannot fire.
type InsufficientHistoryError struct{ Have int }

func (e *InsufficientHistoryError) Error() string {
	return fmt.Sprintf("trigger: insufficient history: %d valid residuals before the tested day, need %d", e.Have, Window)
}

// Closes is a symbol's daily closes keyed by session date (00:00 UTC on the
// New York date, as candles stores them).
type Closes map[time.Time]float64

// NewCloses builds Closes from daily candles.
func NewCloses(bars []source.Candle) Closes {
	c := make(Closes, len(bars))
	for _, b := range bars {
		c[day(b.Time)] = b.Close
	}
	return c
}

func day(t time.Time) time.Time { return t.UTC().Truncate(24 * time.Hour) }

// Sessions is the calendar: every date on which any given symbol has a bar,
// ascending. A date is a session if any symbol traded it, so a symbol missing
// one is visibly missing it rather than silently skipping it.
func Sessions(all map[string]Closes) []time.Time {
	seen := map[time.Time]struct{}{}
	for _, c := range all {
		for d := range c {
			seen[d] = struct{}{}
		}
	}
	out := make([]time.Time, 0, len(seen))
	for d := range seen {
		out = append(out, d)
	}
	slices.SortFunc(out, func(a, b time.Time) int { return a.Compare(b) })
	return out
}

// Point is one day's returns.
type Point struct {
	Date      time.Time
	Return    float64
	Benchmark *float64 // nil when the symbol has no benchmark
	Residual  float64
}

// Result is the full figure for one sensor on one tested day.
type Result struct {
	Symbol    string
	Benchmark string // empty when none
	Date      time.Time
	Point
	WindowStart time.Time // the oldest session in the window
	Sigma       float64
	Z           float64
	Windowed    int // residuals used, always Window on success
}

// pointAt returns the day's returns, or false if either leg lacks the day or
// the session before it. sessions[i-1] is the previous session.
func pointAt(sessions []time.Time, i int, sym, bench Closes) (Point, bool) {
	if i < 1 {
		return Point{}, false
	}
	t, p := sessions[i], sessions[i-1]
	c1, ok1 := sym[t]
	c0, ok0 := sym[p]
	if !ok1 || !ok0 || c0 == 0 {
		return Point{}, false
	}
	pt := Point{Date: t, Return: c1/c0 - 1}
	pt.Residual = pt.Return
	if bench != nil {
		b1, okb1 := bench[t]
		b0, okb0 := bench[p]
		if !okb1 || !okb0 || b0 == 0 {
			return Point{}, false
		}
		br := b1/b0 - 1
		pt.Benchmark = &br
		pt.Residual = pt.Return - br
	}
	return pt, true
}

// Evaluate computes the figure for the session at sessions[at]. bench is nil
// for a symbol with no benchmark, whose residual is its raw return.
func Evaluate(sessions []time.Time, at int, symbol, benchmark string, sym, bench Closes) (Result, error) {
	res := Result{Symbol: symbol, Benchmark: benchmark}
	if at < 0 || at >= len(sessions) {
		return res, fmt.Errorf("trigger: session index %d out of range", at)
	}
	res.Date = sessions[at]
	if benchmark != "" && bench == nil {
		return res, fmt.Errorf("trigger: %s has benchmark %s but no closes for it", symbol, benchmark)
	}
	pt, ok := pointAt(sessions, at, sym, bench)
	if !ok {
		return res, ErrNoReturn
	}
	res.Point = pt

	window := make([]float64, 0, Window)
	for i := at - 1; i >= 1 && len(window) < Window; i-- {
		if p, ok := pointAt(sessions, i, sym, bench); ok {
			window = append(window, p.Residual)
			res.WindowStart = sessions[i]
		}
	}
	if len(window) < Window {
		return res, &InsufficientHistoryError{Have: len(window)}
	}
	res.Windowed = len(window)
	res.Sigma = SampleStdDev(window)
	if res.Sigma == 0 {
		return res, ErrZeroSigma
	}
	res.Z = pt.Residual / res.Sigma
	return res, nil
}

// SampleStdDev is the sample standard deviation (n-1) about the mean, what a
// spreadsheet's STDEV returns. It is 0 for fewer than two values.
func SampleStdDev(v []float64) float64 {
	if len(v) < 2 {
		return 0
	}
	var mean float64
	for _, x := range v {
		mean += x
	}
	mean /= float64(len(v))
	var ss float64
	for _, x := range v {
		d := x - mean
		ss += d * d
	}
	return math.Sqrt(ss / float64(len(v)-1))
}

// CandleReader is what Load needs from Timescale.
type CandleReader interface {
	RecentCandles(ctx context.Context, symbol, interval string, limit int) ([]source.Candle, error)
}

// Load reads the recent daily closes of every symbol in symbols.
func Load(ctx context.Context, r CandleReader, symbols []string) (map[string]Closes, error) {
	out := make(map[string]Closes, len(symbols))
	for _, s := range symbols {
		bars, err := r.RecentCandles(ctx, s, DailyInterval, loadLimit)
		if err != nil {
			return nil, fmt.Errorf("trigger: load %s: %w", s, err)
		}
		out[s] = NewCloses(bars)
	}
	return out, nil
}

// RangeReader reads daily candles in [from, to), oldest first.
type RangeReader interface {
	Candles(ctx context.Context, symbol, interval string, from, to time.Time) ([]source.Candle, error)
}

// loadSpan is how far before a session LoadAsOf reads: the window, the two
// closes around the tested day, and room for weekends, holidays and gaps.
const loadSpan = 150 * 24 * time.Hour

// LoadAsOf reads the daily closes of every symbol up to and including session,
// so a past session is judged by the data that preceded it and not by
// whatever has been written since.
func LoadAsOf(ctx context.Context, r RangeReader, symbols []string, session time.Time) (map[string]Closes, error) {
	to := day(session).Add(24 * time.Hour)
	out := make(map[string]Closes, len(symbols))
	for _, s := range symbols {
		bars, err := r.Candles(ctx, s, DailyInterval, to.Add(-loadSpan), to)
		if err != nil {
			return nil, fmt.Errorf("trigger: load %s: %w", s, err)
		}
		out[s] = NewCloses(bars)
	}
	return out, nil
}

// EvaluateLatest evaluates every sensor on the newest session in the data.
func EvaluateLatest(all map[string]Closes, sensors []string) (results []Result, errs map[string]error) {
	sessions := Sessions(all)
	if len(sessions) == 0 {
		return nil, map[string]error{}
	}
	return EvaluateOn(all, sensors, sessions[len(sessions)-1])
}

// EvaluateOn evaluates every sensor on the given session, using the mapped
// benchmark. Sensors that cannot be evaluated are returned with their error in
// errs, keyed by symbol. A session no symbol has a bar for evaluates nothing
// and reports ErrNoSession for every sensor.
func EvaluateOn(all map[string]Closes, sensors []string, session time.Time) (results []Result, errs map[string]error) {
	errs = map[string]error{}
	sessions := Sessions(all)
	at, found := slices.BinarySearchFunc(sessions, day(session), func(a, b time.Time) int { return a.Compare(b) })
	if !found {
		for _, s := range sensors {
			errs[s] = ErrNoSession
		}
		return nil, errs
	}
	for _, s := range sensors {
		b := Benchmarks[s]
		var bc Closes
		if b != "" {
			bc = all[b]
		}
		r, err := Evaluate(sessions, at, s, b, all[s], bc)
		if err != nil {
			errs[s] = err
			continue
		}
		results = append(results, r)
	}
	return results, errs
}

// LiveFrom is the first session the trigger judged in production (the deploy of
// 17 October judged Friday the 16th). Daily bars exist from 1 July, but nothing
// before this date was ever tested, so a count of events is only a rate over the
// sessions from here (phase16.md step 2, decision 5). `ticker trigger` refuses to
// record a session before it, so in production no event predates it; only the e2e
// fixture (internal/fixture), which judges the backfill, writes earlier ones.
var LiveFrom = time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
