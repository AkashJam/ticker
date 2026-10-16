package trigger

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/source"
)

var start = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

// series builds n consecutive daily dates (calendar days: the package only
// cares about order) and closes that apply returns[i] on day i+1 from 100.
func series(n int, returns func(i int) float64) ([]time.Time, Closes) {
	dates := make([]time.Time, n)
	c := Closes{}
	px := 100.0
	for i := range n {
		dates[i] = start.AddDate(0, 0, i)
		if i > 0 {
			px *= 1 + returns(i)
		}
		c[dates[i]] = px
	}
	return dates, c
}

func alt(i int) float64 {
	if i%2 == 0 {
		return 0.01
	}
	return -0.01
}

func near(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("%s = %.12f, want %.12f", name, got, want)
	}
}

func TestSampleStdDev(t *testing.T) {
	// 2,4,4,4,5,5,7,9: mean 5, sum of squares 32, sample variance 32/7.
	near(t, "stddev", SampleStdDev([]float64{2, 4, 4, 4, 5, 5, 7, 9}), math.Sqrt(32.0/7))
	if SampleStdDev([]float64{1}) != 0 || SampleStdDev(nil) != 0 {
		t.Error("fewer than two values must give 0")
	}
}

// Sixty residuals of alternating +-1% about a mean of 0 give
// sigma = 0.01*sqrt(60/59). The tested day moves 5%, and must not enter sigma.
func TestEvaluateExcludesTestedDay(t *testing.T) {
	const n = Window + 2 // closes: day 0, sixty window returns, the tested day
	dates, sym := series(n, func(i int) float64 {
		if i == n-1 {
			return 0.05
		}
		return alt(i)
	})
	// i=1..60 alternate -1%,+1%,...: thirty of each, mean exactly 0.
	r, err := Evaluate(dates, n-1, "X", "", sym, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSigma := 0.01 * math.Sqrt(60.0/59)
	near(t, "sigma", r.Sigma, wantSigma)
	near(t, "z", r.Z, 0.05/wantSigma)
	near(t, "residual", r.Residual, 0.05)
	if r.Benchmark != "" || r.Point.Benchmark != nil {
		t.Error("no benchmark expected")
	}
	if r.Windowed != Window || !r.WindowStart.Equal(dates[1]) {
		t.Errorf("window = %d from %s, want %d from %s", r.Windowed, r.WindowStart, Window, dates[1])
	}
}

func TestEvaluateSubtractsBenchmark(t *testing.T) {
	const n = Window + 2
	dates, bench := series(n, func(int) float64 { return 0.02 })
	// The symbol moves 2% more than the benchmark every day, plus the alternation.
	_, sym := series(n, func(i int) float64 {
		if i == n-1 {
			return 0.02 + 0.03
		}
		return (1+0.02)*(1+alt(i)) - 1
	})
	r, err := Evaluate(dates, n-1, "X", "B", sym, bench)
	if err != nil {
		t.Fatal(err)
	}
	if r.Point.Benchmark == nil {
		t.Fatal("benchmark return missing")
	}
	near(t, "benchmark return", *r.Point.Benchmark, 0.02)
	near(t, "residual", r.Residual, 0.03)
	if r.Sigma <= 0 {
		t.Error("sigma must be positive")
	}
}

func TestEvaluateBenchmarkNamedButMissing(t *testing.T) {
	dates, sym := series(Window+2, alt)
	if _, err := Evaluate(dates, Window+1, "X", "B", sym, nil); err == nil {
		t.Error("a named benchmark with no closes must not fall back to the raw return")
	}
}

func TestEvaluateInsufficientHistory(t *testing.T) {
	// 61 sessions: tested day is the 61st, only 59 returns precede it.
	dates, sym := series(Window+1, alt)
	_, err := Evaluate(dates, Window, "X", "", sym, nil)
	var ih *InsufficientHistoryError
	if !errors.As(err, &ih) || ih.Have != Window-1 {
		t.Fatalf("err = %v, want insufficient history with %d", err, Window-1)
	}
}

func TestEvaluateGapIsNotBridged(t *testing.T) {
	// 62 sessions, 60 returns before the tested day. Remove one close in the
	// middle: it invalidates two returns (into and out of the gap), leaving 58.
	dates, sym := series(Window+2, alt)
	delete(sym, dates[30])
	_, err := Evaluate(dates, Window+1, "X", "", sym, nil)
	var ih *InsufficientHistoryError
	if !errors.As(err, &ih) || ih.Have != Window-2 {
		t.Fatalf("err = %v, want insufficient history with %d", err, Window-2)
	}
}

func TestEvaluateGapWithEnoughHistoryReachesBack(t *testing.T) {
	// 70 sessions with the same gap: sixty valid residuals still exist, so the
	// window reaches further back than sixty sessions.
	n := Window + 10
	dates, sym := series(n, alt)
	delete(sym, dates[n-20])
	r, err := Evaluate(dates, n-1, "X", "", sym, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Windowed != Window {
		t.Errorf("windowed = %d", r.Windowed)
	}
	// 69 candidate returns before the tested day (idx 1..68 = 68), minus 2 invalid = 66;
	// the oldest 6 are left out, so the window starts at session index 1+6.
	if !r.WindowStart.Equal(dates[7]) {
		t.Errorf("window starts %s, want %s", r.WindowStart, dates[7])
	}
}

func TestEvaluateTestedDayMissing(t *testing.T) {
	dates, sym := series(Window+2, alt)
	delete(sym, dates[Window+1])
	if _, err := Evaluate(dates, Window+1, "X", "", sym, nil); !errors.Is(err, ErrNoReturn) {
		t.Errorf("err = %v, want ErrNoReturn", err)
	}
}

func TestEvaluateZeroSigma(t *testing.T) {
	dates, sym := series(Window+2, func(int) float64 { return 0 })
	if _, err := Evaluate(dates, Window+1, "X", "", sym, nil); !errors.Is(err, ErrZeroSigma) {
		t.Errorf("err = %v, want ErrZeroSigma", err)
	}
}

func TestSessionsIsTheUnion(t *testing.T) {
	a := Closes{start: 1, start.AddDate(0, 0, 2): 1}
	b := Closes{start.AddDate(0, 0, 1): 1, start.AddDate(0, 0, 2): 1}
	got := Sessions(map[string]Closes{"a": a, "b": b})
	if len(got) != 3 || !got[0].Equal(start) || !got[2].Equal(start.AddDate(0, 0, 2)) {
		t.Errorf("sessions = %v", got)
	}
}

func TestNewClosesKeysByUTCDay(t *testing.T) {
	c := NewCloses([]source.Candle{{Time: start, Close: 7}})
	if c[start] != 7 {
		t.Errorf("closes = %v", c)
	}
}

// The mapping must name real sensors and real sector ETFs, and every sensor
// must be either mapped or carry a reason for having no benchmark.
func TestBenchmarkMapping(t *testing.T) {
	etf := map[string]bool{}
	for _, s := range source.SweepUniverse {
		if s.Type == source.AssetTypeETF {
			etf[s.Symbol] = true
		}
	}
	for _, s := range source.StreamUniverse() {
		b, mapped := Benchmarks[s.Symbol]
		_, reason := NoBenchmarkReason[s.Symbol]
		if mapped == reason {
			t.Errorf("%s: mapped=%v, has a no-benchmark reason=%v; want exactly one", s.Symbol, mapped, reason)
		}
		if mapped && !etf[b] {
			t.Errorf("%s benchmark %s is not a sector ETF in the universe", s.Symbol, b)
		}
	}
	if len(Benchmarks)+len(NoBenchmarkReason) != len(source.StreamUniverse()) {
		t.Error("the mapping names a symbol that is not a sensor")
	}
}
