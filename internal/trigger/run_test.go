package trigger

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/source"
)

var ny = mustLoc("America/New_York")

func mustLoc(name string) *time.Location {
	l, err := time.LoadLocation(name)
	if err != nil {
		panic(err)
	}
	return l
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeCandles serves per-symbol daily closes, honouring the [from, to) bounds.
type fakeCandles map[string]Closes

func (f fakeCandles) Candles(_ context.Context, symbol, _ string, from, to time.Time) ([]source.Candle, error) {
	var out []source.Candle
	for d, c := range f[symbol] {
		if !d.Before(from) && d.Before(to) {
			out = append(out, source.Candle{Time: d, Symbol: symbol, Interval: "1d", Close: c})
		}
	}
	return out, nil
}

type fakeEvents struct {
	rows    map[string]Trip
	inserts int
	failOn  string

	splitWrites    int
	coverageWrites int
	clock          func() time.Time
}

func newFakeEvents() *fakeEvents { return &fakeEvents{rows: map[string]Trip{}} }

func key(sym string, d time.Time) string { return sym + "|" + d.Format(time.DateOnly) }

func (f *fakeEvents) InsertEvent(_ context.Context, t Trip) (bool, error) {
	if t.Symbol == f.failOn {
		return false, errors.New("boom")
	}
	k := key(t.Symbol, t.SessionDate)
	if _, ok := f.rows[k]; ok {
		return false, nil
	}
	f.rows[k] = t
	f.inserts++
	return true, nil
}

func (f *fakeEvents) SetSplit(_ context.Context, sym string, d time.Time, label *string, split []byte) (bool, error) {
	k := key(sym, d)
	row, ok := f.rows[k]
	if !ok || row.Split != nil {
		return false, nil
	}
	row.SplitLabel, row.Split = label, split
	f.rows[k] = row
	f.splitWrites++
	return true, nil
}

func (f *fakeEvents) SetCoverage(_ context.Context, sym string, d time.Time, cov []byte) error {
	k := key(sym, d)
	row, ok := f.rows[k]
	if !ok {
		return errors.New("no such event")
	}
	row.Coverage = cov
	now := f.now()
	row.CoverageFetchedAt = &now
	f.rows[k] = row
	f.coverageWrites++
	return nil
}

func (f *fakeEvents) PendingCoverage(_ context.Context, before time.Time) ([]Trip, error) {
	var out []Trip
	for _, row := range f.rows {
		if !row.SessionDate.Before(before) {
			continue
		}
		if row.Coverage == nil || !strings.Contains(string(row.Coverage), `"complete":true`) {
			out = append(out, row)
		}
	}
	slices.SortFunc(out, func(a, b Trip) int { return a.SessionDate.Compare(b.SessionDate) })
	return out, nil
}

func (f *fakeEvents) now() time.Time {
	if f.clock != nil {
		return f.clock()
	}
	return time.Now()
}

func (f *fakeEvents) Event(_ context.Context, sym string, d time.Time) (Trip, bool, error) {
	t, ok := f.rows[key(sym, d)]
	return t, ok, nil
}

// market builds closes for all 28 symbols over Window+3 sessions: the 18
// sensors alternate +-1% (sigma 0.01*sqrt(60/59)) and the 10 sector ETFs stay
// flat, and moves[sym] is the tested day's return. The tested day is the last
// date.
func market(moves map[string]float64) (fakeCandles, time.Time) {
	const n = Window + 3
	f := fakeCandles{}
	var last time.Time
	sensor := map[string]bool{}
	for _, s := range source.StreamUniverse() {
		sensor[s.Symbol] = true
	}
	for _, code := range source.SweepSymbolCodes() {
		_, c := series(n, func(i int) float64 {
			switch {
			case i == n-1:
				return moves[code]
			case sensor[code]:
				return alt(i)
			}
			return 0
		})
		f[code] = c
	}
	last = start.AddDate(0, 0, n-1)
	return f, last
}

func runner(c fakeCandles, ev EventStore, thin map[string]bool) *Runner {
	return &Runner{
		Candles: c, Events: ev, Loc: ny, Threshold: 2.5, Log: quiet(),
		Thin: func(context.Context) (map[string]bool, error) { return thin, nil },
	}
}

func TestFireIsStrict(t *testing.T) {
	for _, tt := range []struct {
		z    float64
		want bool
	}{{2.5, false}, {-2.5, false}, {2.5000001, true}, {-2.6, true}, {0, false}, {2.4, false}} {
		if got := Fire(tt.z, 2.5); got != tt.want {
			t.Errorf("Fire(%v) = %v, want %v", tt.z, got, tt.want)
		}
	}
}

func TestRunRecordsOnlyTheTrips(t *testing.T) {
	// CAT falls 8% against a flat EXI; AAPL rises 6% with no benchmark; RY
	// moves 1% against a flat IXG, well inside its sigma.
	c, last := market(map[string]float64{"CAT": -0.08, "AAPL": 0.06, "RY": 0.01, "KB": 0.09})
	ev := newFakeEvents()
	sum, err := runner(c, ev, map[string]bool{"KB": true}).Run(context.Background(), last)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Evaluated != 18 || len(sum.Skipped) != 0 {
		t.Fatalf("evaluated %d, skipped %v", sum.Evaluated, sum.Skipped)
	}
	got := map[string]Trip{}
	for _, tr := range sum.Trips {
		got[tr.Symbol] = tr
	}
	if len(got) != 3 || sum.Inserted != 3 || ev.inserts != 3 {
		t.Fatalf("trips %v, inserted %d", slices.Sorted(mapKeys(got)), sum.Inserted)
	}

	cat := got["CAT"]
	if cat.Benchmark == nil || *cat.Benchmark != "EXI" || cat.BenchmarkReturn == nil || cat.BenchmarkReason != nil {
		t.Errorf("CAT benchmark columns = %v %v %v", cat.Benchmark, cat.BenchmarkReturn, cat.BenchmarkReason)
	}
	if cat.Z >= 0 || !slices.Equal(cat.Caveats, []string{CaveatSelfWeight}) {
		t.Errorf("CAT z=%v caveats=%v", cat.Z, cat.Caveats)
	}
	if cat.WindowSessions != Window || cat.Threshold != 2.5 {
		t.Errorf("CAT window=%d threshold=%v", cat.WindowSessions, cat.Threshold)
	}
	if l := cat.ClosedAt.In(ny); l.Hour() != 16 || l.Minute() != 0 || l.Format(time.DateOnly) != cat.SessionDate.Format(time.DateOnly) {
		t.Errorf("closed_at = %s, want 16:00 New York on the session date", l)
	}

	aapl := got["AAPL"]
	if aapl.Benchmark != nil || aapl.BenchmarkReturn != nil || aapl.BenchmarkReason == nil || len(aapl.Caveats) != 0 {
		t.Errorf("AAPL = %+v: no benchmark, a reason, and no self_weight", aapl)
	}
	if math.Abs(aapl.Residual-aapl.SymbolReturn) > 1e-12 {
		t.Errorf("AAPL residual %v != raw return %v", aapl.Residual, aapl.SymbolReturn)
	}

	if kb := got["KB"]; !slices.Equal(kb.Caveats, []string{CaveatThinListing}) {
		t.Errorf("KB caveats = %v", kb.Caveats)
	}
	if _, ok := got["RY"]; ok {
		t.Error("RY, inside its sigma, must not trip")
	}
}

func mapKeys[V any](m map[string]V) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

func TestRunIsIdempotentAndKeepsTheRecord(t *testing.T) {
	c, last := market(map[string]float64{"CAT": -0.08})
	ev := newFakeEvents()
	r := runner(c, ev, nil)
	if _, err := r.Run(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	sum, err := r.Run(context.Background(), last)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Inserted != 0 || sum.Existing != 1 || ev.inserts != 1 || len(sum.Mismatched) != 0 {
		t.Errorf("second run: %+v, inserts %d", sum, ev.inserts)
	}

	// The bar is corrected afterwards: the fresh figures differ, the record stays.
	k := key("CAT", last)
	recorded := ev.rows[k]
	c["CAT"][last] *= 0.97
	sum, err = r.Run(context.Background(), last)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(sum.Mismatched, []string{"CAT"}) {
		t.Errorf("mismatched = %v, want [CAT]", sum.Mismatched)
	}
	if ev.rows[k].Z != recorded.Z || ev.rows[k].Residual != recorded.Residual {
		t.Error("the recorded event was rewritten")
	}
}

func TestRunSkipsWhatItCannotJudgeAndJudgesTheRest(t *testing.T) {
	// A partial sweep: KB and the IXG benchmark have no bar on the tested day.
	c, last := market(map[string]float64{"CAT": -0.08})
	delete(c["KB"], last)
	delete(c["IXG"], last)
	sum, err := runner(c, newFakeEvents(), nil).Run(context.Background(), last)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Skipped["KB"] != "no_return" || sum.Skipped["RY"] != "no_return" {
		t.Errorf("skipped = %v", sum.Skipped)
	}
	if len(sum.Trips) != 1 || sum.Trips[0].Symbol != "CAT" {
		t.Errorf("trips = %+v, want CAT alone", sum.Trips)
	}
}

func TestRunOnASessionNobodyTraded(t *testing.T) {
	c, last := market(nil)
	sum, err := runner(c, newFakeEvents(), nil).Run(context.Background(), last.AddDate(0, 0, 5))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Evaluated != 0 || sum.Skipped["RY"] != "no_session" {
		t.Errorf("summary = %+v", sum)
	}
}

func TestRunJudgesAPastSessionByWhatPrecededIt(t *testing.T) {
	// Data after the session must not matter: a crash on the next day is not
	// part of the window, and the session still trips.
	c, last := market(map[string]float64{"CAT": -0.08})
	next := last.AddDate(0, 0, 1)
	for sym, cl := range c {
		cl[next] = cl[last] * 0.5
		c[sym] = cl
	}
	sum, err := runner(c, newFakeEvents(), nil).Run(context.Background(), last)
	if err != nil {
		t.Fatal(err)
	}
	if len(sum.Trips) != 1 || sum.Trips[0].Symbol != "CAT" {
		t.Errorf("trips = %+v", sum.Trips)
	}
}

func TestRunRecordsNothingWhenTheReferenceLookupFails(t *testing.T) {
	c, last := market(map[string]float64{"CAT": -0.08})
	ev := newFakeEvents()
	r := runner(c, ev, nil)
	r.Thin = func(context.Context) (map[string]bool, error) { return nil, errors.New("no table") }
	if _, err := r.Run(context.Background(), last); err == nil {
		t.Fatal("want an error")
	}
	if ev.inserts != 0 {
		t.Errorf("%d events written without their caveats", ev.inserts)
	}
}

func TestRunStopsOnAStoreError(t *testing.T) {
	c, last := market(map[string]float64{"CAT": -0.08})
	ev := newFakeEvents()
	ev.failOn = "CAT"
	if _, err := runner(c, ev, nil).Run(context.Background(), last); err == nil {
		t.Fatal("want an error")
	}
}

func TestReadOnlyWritesNothingAndSaysWhatWouldBeNew(t *testing.T) {
	c, last := market(map[string]float64{"CAT": -0.08})
	ev := newFakeEvents()
	r := runner(c, ReadOnly(ev), nil)
	sum, err := r.Run(context.Background(), last)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Inserted != 1 || ev.inserts != 0 {
		t.Errorf("dry run: would insert %d, wrote %d", sum.Inserted, ev.inserts)
	}
	real := runner(c, ev, nil)
	if _, err := real.Run(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	sum, _ = r.Run(context.Background(), last)
	if sum.Inserted != 0 || sum.Existing != 1 {
		t.Errorf("dry run after a real one: %+v", sum)
	}
}

func TestValidateCaveats(t *testing.T) {
	if err := ValidateCaveats([]string{CaveatThinListing, CaveatSelfWeight}); err != nil {
		t.Error(err)
	}
	if ValidateCaveats([]string{"made_up"}) == nil {
		t.Error("an unknown caveat must be refused")
	}
}

// Every self_weight sensor is a mapped sensor, and none is AAPL.
func TestSelfWeightNamesBenchmarkedSensors(t *testing.T) {
	for s := range SelfWeight {
		if _, ok := Benchmarks[s]; !ok {
			t.Errorf("%s is self-weight but has no benchmark", s)
		}
	}
	if SelfWeight["AAPL"] {
		t.Error("AAPL has no benchmark subtracted, so it carries no self_weight")
	}
}

func TestReasonOf(t *testing.T) {
	for _, tt := range []struct {
		err  error
		want string
	}{
		{ErrNoSession, "no_session"}, {ErrNoReturn, "no_return"}, {ErrZeroSigma, "zero_sigma"},
		{&InsufficientHistoryError{Have: 3}, "insufficient_history"}, {errors.New("x"), "error"},
	} {
		if got := ReasonOf(tt.err); got != tt.want {
			t.Errorf("ReasonOf(%v) = %q, want %q", tt.err, got, tt.want)
		}
	}
}
