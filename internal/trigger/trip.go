package trigger

import (
	"fmt"
	"math"
	"slices"
	"time"
)

// The caveats an event can carry, as the enum list the API will serve (the
// view owns the wording). Validated here and not by the database, so a new one
// needs no migration (phase15.md step 1, decision 7).
const (
	// CaveatThinListing marks a sensor whose US listing trades thinly (#93).
	CaveatThinListing = "thin_listing"
	// CaveatSelfWeight marks a sensor that is a large holding of its own
	// benchmark, so the residual partly subtracts the sensor from itself (#110).
	CaveatSelfWeight = "self_weight"
)

// KnownCaveats is the closed set of caveats.
var KnownCaveats = []string{CaveatThinListing, CaveatSelfWeight}

// SelfWeight is the sensors that are large holdings of their own benchmark
// (bearing.md #110, holdings of 12 Oct 2026): XOM 18% of IXC, NVDA 13% of IXN,
// JPM 5.8% of IXG, CAT 3.8% of EXI. AAPL (12% of IXN) is absent on purpose: it
// has no benchmark subtracted, so there is nothing to double count.
var SelfWeight = map[string]bool{"XOM": true, "NVDA": true, "JPM": true, "CAT": true}

// ValidateCaveats reports a caveat outside the known set.
func ValidateCaveats(c []string) error {
	for _, v := range c {
		if !slices.Contains(KnownCaveats, v) {
			return fmt.Errorf("trigger: unknown caveat %q", v)
		}
	}
	return nil
}

// Trip is one event's trigger columns, as the events table stores them: every
// input a reader needs to recompute the figure from the daily candles.
type Trip struct {
	Symbol      string
	SessionDate time.Time // the New York session, midnight UTC
	ClosedAt    time.Time // the session's 16:00 New York close

	SymbolReturn    float64
	Benchmark       *string
	BenchmarkReturn *float64
	BenchmarkReason *string
	Residual        float64
	Sigma           float64
	Z               float64
	Threshold       float64
	WindowSessions  int
	Caveats         []string

	// SplitLabel and Split are the currency split, set once by step 4 and nil
	// until then. They are filled when a recorded event is read back; NewTrip
	// leaves them nil.
	SplitLabel *string
	Split      []byte

	// Coverage is the news and filings fetched for the trip (internal/coverage),
	// nil until the first fetch; CoverageFetchedAt is when it was last written.
	Coverage          []byte
	CoverageFetchedAt *time.Time
}

// SplitPending reports whether the event has not yet been split.
func (t Trip) SplitPending() bool { return t.Split == nil }

// Fire reports whether a z passes the threshold. The comparison is strict: a
// move of exactly the threshold does not trip.
func Fire(z, threshold float64) bool { return math.Abs(z) > threshold }

// NewTrip builds the event for a result that fired. thin is whether the
// sensor's listing is tagged thin in the reference table. loc is New York; the
// close is stored as 16:00 there, so a half-day (13:00) is not distinguished
// (phase15.md step 3).
func NewTrip(r Result, threshold float64, thin bool, loc *time.Location) Trip {
	y, m, d := r.Date.Date()
	t := Trip{
		Symbol:         r.Symbol,
		SessionDate:    r.Date,
		ClosedAt:       time.Date(y, m, d, 16, 0, 0, 0, loc),
		SymbolReturn:   r.Return,
		Residual:       r.Residual,
		Sigma:          r.Sigma,
		Z:              r.Z,
		Threshold:      threshold,
		WindowSessions: r.Windowed,
		Caveats:        []string{},
	}
	if r.Benchmark != "" {
		b := r.Benchmark
		t.Benchmark = &b
		t.BenchmarkReturn = r.Point.Benchmark
	} else if reason, ok := NoBenchmarkReason[r.Symbol]; ok {
		t.BenchmarkReason = &reason
	} else {
		reason := "no benchmark is mapped for this sensor"
		t.BenchmarkReason = &reason
	}
	if thin {
		t.Caveats = append(t.Caveats, CaveatThinListing)
	}
	if SelfWeight[r.Symbol] {
		t.Caveats = append(t.Caveats, CaveatSelfWeight)
	}
	return t
}

// Differs reports whether two records of the same event disagree on the
// figures that fired it (the threshold is allowed to have moved).
func (t Trip) Differs(o Trip) bool {
	const eps = 1e-9
	return math.Abs(t.Residual-o.Residual) > eps ||
		math.Abs(t.Sigma-o.Sigma) > eps ||
		math.Abs(t.Z-o.Z) > 1e-6
}
