package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

// splitFixture builds Window+2 sessions for one sensor with no benchmark.
// Over sessions 1..60 the currency part alternates -1%/+1% and the stock part
// +-2% (each averages exactly zero), so sigma_c = 0.01*sqrt(60/59) and
// sigma_s = 0.02*sqrt(60/59). The tested session (61) has the given parts. The
// sensor's residual is c + s, and the rate series is built to give exactly c.
func splitFixture(cTest, sTest float64) (sessions []time.Time, sym Closes, rates RateSet) {
	const n = Window + 2
	sessions = make([]time.Time, n)
	sym = Closes{}
	per := map[time.Time]float64{}
	px, lpd := 100.0, 20.0 // dollars and pesos per dollar
	for i := range n {
		d := start.AddDate(0, 0, i)
		sessions[i] = d
		if i > 0 {
			c, s := alt(i), 2*alt(i) // the two move together; only their sizes matter here
			if i == n-1 {
				c, s = cTest, sTest
			}
			px *= 1 + c + s
			lpd /= 1 + c // dollars per peso rises by c
		}
		sym[d] = px
		per[d] = lpd
	}
	return sessions, sym, RateSet{Currency: "MXN", PerUSD: per, Source: "daily", FixDate: sessions[n-1]}
}

func near2(t *testing.T, name string, got, want, tol float64) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Errorf("%s = %.10f, want %.10f", name, got, want)
	}
}

func TestSplitArithmetic(t *testing.T) {
	sessions, sym, rates := splitFixture(0.03, 0.01)
	got, err := Split(sessions, len(sessions)-1, "AMX", "", sym, nil, rates, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "ok" {
		t.Fatalf("result = %+v", got)
	}
	sc := 0.01 * math.Sqrt(60.0/59)
	ss := 0.02 * math.Sqrt(60.0/59)
	near2(t, "currency part", got.CurrencyPart.Value, 0.03, 1e-9)
	near2(t, "currency sigma", got.CurrencyPart.Sigma, sc, 1e-9)
	near2(t, "currency z", got.CurrencyPart.Z, 0.03/sc, 1e-6)
	near2(t, "stock part", got.StockPart.Value, 0.01, 1e-9)
	near2(t, "stock sigma", got.StockPart.Sigma, ss, 1e-9)
	near2(t, "stock z", got.StockPart.Z, 0.01/ss, 1e-6)
	near2(t, "residual is the sum", got.Residual, got.CurrencyPart.Value+got.StockPart.Value, 1e-12)
	if got.Label != LabelCurrencyLed {
		t.Errorf("label = %s, want currency-led (z %v / %v)", got.Label, got.CurrencyPart.Z, got.StockPart.Z)
	}
	if got.Window != Window || got.Threshold != 2 || got.Currency != "MXN" || got.Source != "daily" {
		t.Errorf("provenance = %+v", got)
	}
	if got.WindowStart != sessions[1].Format(time.DateOnly) || got.FixDate != sessions[len(sessions)-1].Format(time.DateOnly) {
		t.Errorf("window start %s, fix %s", got.WindowStart, got.FixDate)
	}
	// The stored rates give the currency part back.
	near2(t, "from the stored rates", got.RatePrevious/got.RateCurrent-1, got.CurrencyPart.Value, 1e-12)
}

func TestSplitLabels(t *testing.T) {
	for _, tt := range []struct {
		name string
		c, s float64
		want string
	}{
		{"only the currency passes", 0.03, 0.01, LabelCurrencyLed},
		{"only the stock passes", 0.005, 0.06, LabelStockLed},
		{"both pass", 0.03, 0.06, LabelBoth},
		{"neither passes alone", 0.015, 0.03, LabelNeither},
		{"a negative part counts the same", -0.03, 0.01, LabelCurrencyLed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sessions, sym, rates := splitFixture(tt.c, tt.s)
			got, err := Split(sessions, len(sessions)-1, "AMX", "", sym, nil, rates, 2)
			if err != nil || got.Label != tt.want {
				t.Fatalf("label = %q (%v), want %q; z %v %v", got.Label, err, tt.want, got.CurrencyPart, got.StockPart)
			}
		})
	}
	if LabelFor(2, 2, 2) != LabelNeither {
		t.Error("exactly the threshold does not count")
	}
}

func TestSplitSubtractsTheBenchmarkFromTheStockPart(t *testing.T) {
	sessions, sym, rates := splitFixture(0, 0)
	// A flat benchmark leaves the residual as the raw return; a benchmark that
	// moves 1% on the tested day takes 1% off the residual and so off the stock part.
	bench := Closes{}
	for i, d := range sessions {
		bench[d] = 100
		if i == len(sessions)-1 {
			bench[d] = 101
		}
	}
	// Re-make the stock so that its tested-day return is 1% with c = 0.
	last := sessions[len(sessions)-1]
	sym[last] = sym[sessions[len(sessions)-2]] * 1.01
	got, err := Split(sessions, len(sessions)-1, "X", "B", sym, bench, rates, 2)
	if err != nil {
		t.Fatal(err)
	}
	near2(t, "residual", got.Residual, 0, 1e-12)
	near2(t, "stock part", got.StockPart.Value, 0, 1e-12)
}

func TestSplitNoneReasons(t *testing.T) {
	sessions, sym, rates := splitFixture(0.03, 0.01)
	at := len(sessions) - 1

	// The tested day has no rate.
	missing := RateSet{PerUSD: map[time.Time]float64{}}
	for d, v := range rates.PerUSD {
		missing.PerUSD[d] = v
	}
	delete(missing.PerUSD, sessions[at])
	if got, _ := Split(sessions, at, "AMX", "", sym, nil, missing, 2); got.Status != "none" || got.Reason != NoSplitNotPublished {
		t.Errorf("missing tested rate: %+v", got)
	}

	// One rate missing inside the window costs two returns: 58 left.
	gap := RateSet{PerUSD: map[time.Time]float64{}}
	for d, v := range rates.PerUSD {
		gap.PerUSD[d] = v
	}
	delete(gap.PerUSD, sessions[30])
	if got, _ := Split(sessions, at, "AMX", "", sym, nil, gap, 2); got.Reason != NoSplitInsufficient {
		t.Errorf("a gap in the window: %+v", got)
	}

	// A flat currency has no sigma.
	flat := RateSet{PerUSD: map[time.Time]float64{}}
	for _, d := range sessions {
		flat.PerUSD[d] = 20
	}
	if got, _ := Split(sessions, at, "AMX", "", sym, nil, flat, 2); got.Reason != NoSplitZeroSigma {
		t.Errorf("a flat currency: %+v", got)
	}

	if _, err := Split(sessions, 0, "AMX", "", sym, nil, rates, 2); err == nil {
		t.Error("session 0 has no previous session")
	}
	if _, err := Split(sessions, at, "AMX", "B", sym, nil, rates, 2); err == nil {
		t.Error("a named benchmark with no closes must be an error")
	}
}

func TestSplitJSONShape(t *testing.T) {
	sessions, sym, rates := splitFixture(0.03, 0.01)
	got, _ := Split(sessions, len(sessions)-1, "AMX", "", sym, nil, rates, 2)
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"status", "label", "currency", "source", "fixDate", "ratePrevious", "rateCurrent", "residual", "currencyPart", "stockPart", "threshold", "window", "windowStart"} {
		if _, ok := m[k]; !ok {
			t.Errorf("split JSON lacks %q: %s", k, b)
		}
	}
	none, _ := json.Marshal(None(NoSplitNoHomeCurrency))
	if string(none) != `{"status":"none","reason":"no_home_currency"}` {
		t.Errorf("none = %s", none)
	}
}

// --- through the runner ---

type fakeRates struct {
	sets  map[string]RateSet
	errs  map[string]error
	calls int
}

func (f *fakeRates) Rates(_ context.Context, symbol string, _ time.Time) (RateSet, error) {
	f.calls++
	if err := f.errs[symbol]; err != nil {
		return RateSet{}, err
	}
	return f.sets[symbol], nil
}

// splitMarket is market() with the AMX rates built to give it a currency part.
func splitMarket(t *testing.T) (fakeCandles, time.Time, RateSet) {
	t.Helper()
	c, last := market(map[string]float64{"AMX": 0.06, "CAT": -0.08})
	per := map[time.Time]float64{}
	lpd := 20.0
	for i := range Window + 3 {
		d := start.AddDate(0, 0, i)
		if i > 0 {
			cc := -0.5 * alt(i) // not the sensor's own move, or the stock part would have no sigma
			if i == Window+2 {
				cc = 0.03
			}
			lpd /= 1 + cc
		}
		per[d] = lpd
	}
	return c, last, RateSet{Currency: "MXN", PerUSD: per, Source: "daily", FixDate: last}
}

func decode(t *testing.T, b []byte) SplitResult {
	t.Helper()
	var s SplitResult
	if err := json.Unmarshal(b, &s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRunSplitsEachTripOnceAndLeavesGlobalNamesWithAReason(t *testing.T) {
	c, last, rs := splitMarket(t)
	ev := newFakeEvents()
	rates := &fakeRates{
		sets: map[string]RateSet{"AMX": rs},
		errs: map[string]error{"CAT": &NoSplitError{Reason: NoSplitNoHomeCurrency}},
	}
	r := runner(c, ev, nil)
	r.Rates, r.SplitThreshold = rates, 2

	sum, err := r.Run(context.Background(), last)
	if err != nil {
		t.Fatal(err)
	}
	amx := ev.rows[key("AMX", last)]
	if amx.SplitLabel == nil || amx.Split == nil {
		t.Fatalf("AMX split not recorded: %+v", amx)
	}
	if got := decode(t, amx.Split); got.Status != "ok" || got.Label != *amx.SplitLabel || got.Currency != "MXN" {
		t.Errorf("AMX split = %+v", got)
	}
	cat := ev.rows[key("CAT", last)]
	if cat.SplitLabel != nil || cat.Split == nil {
		t.Fatalf("CAT: label %v, split %s; want no label and a final reason", cat.SplitLabel, cat.Split)
	}
	if got := decode(t, cat.Split); got.Status != "none" || got.Reason != NoSplitNoHomeCurrency {
		t.Errorf("CAT split = %+v", got)
	}
	if len(sum.Splits) != 2 || ev.splitWrites != 2 {
		t.Errorf("splits %d, writes %d", len(sum.Splits), ev.splitWrites)
	}

	// A second run neither refetches rates nor rewrites a split.
	calls := rates.calls
	if _, err := r.Run(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	if rates.calls != calls || ev.splitWrites != 2 {
		t.Errorf("second run: %d new rate calls, %d writes", rates.calls-calls, ev.splitWrites)
	}
}

func TestRunATransientRateFailureLeavesTheTripAndRetries(t *testing.T) {
	c, last, rs := splitMarket(t)
	ev := newFakeEvents()
	rates := &fakeRates{sets: map[string]RateSet{"AMX": rs}, errs: map[string]error{"AMX": errors.New("ecb unreachable")}}
	r := runner(c, ev, nil)
	r.Rates, r.SplitThreshold = rates, 2

	if _, err := r.Run(context.Background(), last); err != nil {
		t.Fatalf("a rate failure must not fail the run: %v", err)
	}
	amx := ev.rows[key("AMX", last)]
	if amx.Symbol == "" || amx.Split != nil || !amx.SplitPending() {
		t.Fatalf("AMX must be recorded with its split pending: %+v", amx)
	}

	// The ECB comes back: the next run fills the pending split, once.
	rates.errs["AMX"] = nil
	sum, err := r.Run(context.Background(), last)
	if err != nil {
		t.Fatal(err)
	}
	if amx = ev.rows[key("AMX", last)]; amx.Split == nil || sum.Splits["AMX"].Status != "ok" {
		t.Errorf("AMX split after the retry: %s", amx.Split)
	}
	if sum.Inserted != 0 {
		t.Errorf("inserted %d on the retry", sum.Inserted)
	}
}

func TestRunWithoutRatesLeavesEverySplitPending(t *testing.T) {
	c, last, _ := splitMarket(t)
	ev := newFakeEvents()
	if _, err := runner(c, ev, nil).Run(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	for k, row := range ev.rows {
		if row.Split != nil {
			t.Errorf("%s has a split with no rate source", k)
		}
	}
}

func TestReadOnlyShowsTheSplitWithoutWritingIt(t *testing.T) {
	c, last, rs := splitMarket(t)
	ev := newFakeEvents()
	r := runner(c, ReadOnly(ev), nil)
	r.Rates, r.SplitThreshold = &fakeRates{sets: map[string]RateSet{"AMX": rs}, errs: map[string]error{"CAT": &NoSplitError{Reason: NoSplitNoHomeCurrency}}}, 2
	sum, err := r.Run(context.Background(), last)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Splits["AMX"].Status != "ok" || ev.splitWrites != 0 || ev.inserts != 0 {
		t.Errorf("dry run: splits %v, writes %d, inserts %d", sum.Splits, ev.splitWrites, ev.inserts)
	}
}

// --- coverage through the runner ---

type fakeCoverage struct {
	calls  []string
	err    error
	stages []string
	prevs  [][]byte
}

// Collect returns a coverage that is complete only after the follow-up, as the
// real one is when no source is in error.
func (f *fakeCoverage) Collect(_ context.Context, ev Trip, prev []byte, stage string) ([]byte, error) {
	f.calls = append(f.calls, ev.Symbol+" "+ev.SessionDate.Format(time.DateOnly))
	f.stages = append(f.stages, stage)
	f.prevs = append(f.prevs, prev)
	if f.err != nil {
		return nil, f.err
	}
	complete := "false"
	if stage == StageFollowUp {
		complete = "true"
	}
	return []byte(`{"stage":"` + stage + `","complete":` + complete + `}`), nil
}

func coverageRunner(t *testing.T, cov CoverageSource, clock *time.Time) (*Runner, *fakeEvents, fakeCandles, time.Time) {
	t.Helper()
	c, last, rs := splitMarket(t)
	ev := newFakeEvents()
	ev.clock = func() time.Time { return *clock }
	r := runner(c, ev, nil)
	r.Rates = &fakeRates{sets: map[string]RateSet{"AMX": rs}, errs: map[string]error{"CAT": &NoSplitError{Reason: NoSplitNoHomeCurrency}}}
	r.SplitThreshold = 2
	r.Coverage = cov
	r.Now = func() time.Time { return *clock }
	return r, ev, c, last
}

func TestRunCollectsCoverageForEachNewTripOnce(t *testing.T) {
	now := time.Date(2026, 9, 1, 20, 30, 0, 0, time.UTC)
	fc := &fakeCoverage{}
	r, ev, _, last := coverageRunner(t, fc, &now)

	sum, err := r.Run(context.Background(), last)
	if err != nil {
		t.Fatal(err)
	}
	if len(fc.calls) != 2 || fc.stages[0] != StageTrip || ev.coverageWrites != 2 || len(sum.Coverages) != 2 {
		t.Fatalf("calls %v, stages %v, writes %d", fc.calls, fc.stages, ev.coverageWrites)
	}
	for _, row := range ev.rows {
		if row.Coverage == nil {
			t.Errorf("%s has no coverage", row.Symbol)
		}
	}

	// A second run for the same session neither refetches nor rewrites.
	if _, err := r.Run(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	if len(fc.calls) != 2 || ev.coverageWrites != 2 {
		t.Errorf("second run: calls %d, writes %d", len(fc.calls), ev.coverageWrites)
	}
}

func TestCoverageSeesTheSplitTheTripJustGot(t *testing.T) {
	now := time.Now()
	var seen []Trip
	r, _, _, last := coverageRunner(t, coverageFunc(func(_ context.Context, ev Trip, _ []byte, _ string) ([]byte, error) {
		seen = append(seen, ev)
		return []byte(`{}`), nil
	}), &now)
	if _, err := r.Run(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	for _, ev := range seen {
		if ev.SplitPending() {
			t.Errorf("%s reached the collector with its split pending though it had just been written", ev.Symbol)
		}
		if ev.Symbol == "AMX" && (ev.SplitLabel == nil || *ev.SplitLabel == "") {
			t.Error("AMX must carry its label")
		}
	}
}

type coverageFunc func(context.Context, Trip, []byte, string) ([]byte, error)

func (f coverageFunc) Collect(ctx context.Context, ev Trip, prev []byte, stage string) ([]byte, error) {
	return f(ctx, ev, prev, stage)
}

func TestTheNextSessionRunFollowsUpEarlierEvents(t *testing.T) {
	now := time.Date(2026, 9, 1, 20, 30, 0, 0, time.UTC)
	fc := &fakeCoverage{}
	r, ev, c, last := coverageRunner(t, fc, &now)
	if _, err := r.Run(context.Background(), last); err != nil {
		t.Fatal(err)
	}

	// The next session: a day later, with bars for it, and no new trips.
	next := last.AddDate(0, 0, 1)
	for sym, cl := range c {
		cl[next] = cl[last]
		c[sym] = cl
	}
	now = now.Add(24 * time.Hour)
	fc.calls, fc.stages, fc.prevs = nil, nil, nil
	if _, err := r.Run(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	if len(fc.calls) != 2 || fc.stages[0] != StageFollowUp || fc.prevs[0] == nil {
		t.Fatalf("follow-up calls %v stages %v: both earlier events get one, with their coverage so far", fc.calls, fc.stages)
	}
	for _, row := range ev.rows {
		if !strings.Contains(string(row.Coverage), `"complete":true`) {
			t.Errorf("%s coverage = %s", row.Symbol, row.Coverage)
		}
	}

	// Complete coverage is not fetched again.
	fc.calls = nil
	now = now.Add(24 * time.Hour)
	if _, err := r.Run(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	if len(fc.calls) != 0 {
		t.Errorf("complete coverage refetched: %v", fc.calls)
	}
}

func TestRestartsDoNotBurnAttemptsWithinTheRetryGap(t *testing.T) {
	now := time.Date(2026, 9, 1, 20, 30, 0, 0, time.UTC)
	fc := &fakeCoverage{}
	r, _, c, last := coverageRunner(t, fc, &now)
	if _, err := r.Run(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	next := last.AddDate(0, 0, 1)
	for sym, cl := range c {
		cl[next] = cl[last]
		c[sym] = cl
	}
	// The next session's run comes an hour after the trip fetch: too soon.
	now = now.Add(time.Hour)
	fc.calls = nil
	if _, err := r.Run(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	if len(fc.calls) != 0 {
		t.Errorf("fetched again %s after the last fetch; the gap is %s", time.Hour, RetryGap)
	}
	now = now.Add(RetryGap)
	if _, err := r.Run(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	if len(fc.calls) != 2 {
		t.Errorf("after the gap the follow-up must run: %v", fc.calls)
	}
}

func TestACoverageFailureDoesNotFailTheRunAndIsRetried(t *testing.T) {
	now := time.Date(2026, 9, 1, 20, 30, 0, 0, time.UTC)
	fc := &fakeCoverage{err: errors.New("config unreadable")}
	r, ev, _, last := coverageRunner(t, fc, &now)
	if _, err := r.Run(context.Background(), last); err != nil {
		t.Fatalf("a coverage failure must not fail the run: %v", err)
	}
	if ev.inserts != 2 || ev.coverageWrites != 0 {
		t.Errorf("inserts %d, coverage writes %d: the trips stand and nothing half-written is stored", ev.inserts, ev.coverageWrites)
	}
	// Recovered: the next run of the same session fetches the missing trip-stage coverage.
	fc.err = nil
	if _, err := r.Run(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	if ev.coverageWrites != 2 {
		t.Errorf("coverage writes = %d after recovery", ev.coverageWrites)
	}
}

func TestADryRunShowsCoverageAndStoresNone(t *testing.T) {
	now := time.Now()
	fc := &fakeCoverage{}
	r, ev, _, last := coverageRunner(t, fc, &now)
	r.Events = ReadOnly(ev)
	sum, err := r.Run(context.Background(), last)
	if err != nil {
		t.Fatal(err)
	}
	if len(sum.Coverages) != 2 || ev.coverageWrites != 0 || ev.inserts != 0 {
		t.Errorf("coverages %d, writes %d, inserts %d", len(sum.Coverages), ev.coverageWrites, ev.inserts)
	}
}

func TestWithoutACoverageSourceNothingIsFetched(t *testing.T) {
	now := time.Now()
	r, ev, _, last := coverageRunner(t, nil, &now)
	r.Coverage = nil
	if _, err := r.Run(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	if ev.coverageWrites != 0 {
		t.Error("no source, no coverage")
	}
}
