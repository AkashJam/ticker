package change

import (
	"math"
	"testing"
)

// A monthly series for 2010-01 to 2012-12 that doubles every year: 100 in
// January 2010, 200 in January 2011, 400 in January 2012, 800 in December 2012.
func monthly() Series {
	v := make([]float64, 36)
	for i := range v {
		v[i] = 100 * math.Pow(2, float64(i)/12)
	}
	return Series{Start: "2010-01", Step: 1, Values: v}
}

func eq(t *testing.T, name string, got *float64, want float64) {
	t.Helper()
	if got == nil || math.Abs(*got-want) > 0.05 {
		if got == nil {
			t.Errorf("%s = nil, want %v", name, want)
			return
		}
		t.Errorf("%s = %v, want %v", name, *got, want)
	}
}

func TestAtAndLast(t *testing.T) {
	s := monthly()
	if v, ok := s.At(2011, 1); !ok || math.Abs(v-200) > 1e-9 {
		t.Errorf("At(2011,1) = %v %v", v, ok)
	}
	if _, ok := s.At(2013, 1); ok {
		t.Error("At past the end succeeded")
	}
	if _, ok := s.At(2009, 12); ok {
		t.Error("At before the start succeeded")
	}
	if y, m := s.Last(); y != 2012 || m != 12 {
		t.Errorf("Last = %d-%d", y, m)
	}
	q := Series{Start: "2010-01", Step: 3, Values: []float64{1, 2, 3, 4, 5}}
	if _, ok := q.At(2010, 2); ok {
		t.Error("a quarterly series answered for a month between quarters")
	}
	if v, ok := q.At(2011, 1); !ok || v != 5 {
		t.Errorf("quarterly At(2011,1) = %v %v", v, ok)
	}
}

func TestAsOfAndLastCompleteYear(t *testing.T) {
	cases := []struct {
		name string
		s    Series
		asOf string
		year int
	}{
		{"monthly to December", Series{Start: "2024-01", Step: 1, Values: make([]float64, 12)}, "2024-12", 2024},
		{"monthly to August", Series{Start: "2025-01", Step: 1, Values: make([]float64, 8)}, "2025-08", 2024},
		{"quarterly to Q4", Series{Start: "2024-01", Step: 3, Values: make([]float64, 4)}, "2024-Q4", 2024},
		{"quarterly to Q1", Series{Start: "2025-01", Step: 3, Values: make([]float64, 1)}, "2025-Q1", 2024},
		{"annual", Series{Start: "2024-01", Step: 12, Values: make([]float64, 1)}, "2024", 2024},
	}
	for _, c := range cases {
		if got := c.s.AsOf(); got != c.asOf {
			t.Errorf("%s: AsOf = %s, want %s", c.name, got, c.asOf)
		}
		if got := c.s.LastCompleteYear(); got != c.year {
			t.Errorf("%s: LastCompleteYear = %d, want %d", c.name, got, c.year)
		}
	}
}

func TestWindowMonthly(t *testing.T) {
	s := monthly()
	// January 2010 (100) to December 2012 (100·2^(35/12) ≈ 755.6).
	eq(t, "2010-2012", s.Window(2010, 2012), 655.1)
	// January 2011 (200) to December 2012.
	eq(t, "2011-2012", s.Window(2011, 2012), 277.5)
	if s.Window(2010, 2013) != nil {
		t.Error("a window past the end was served")
	}
}

func TestWindowQuarterly(t *testing.T) {
	// Q1 2010 = 100, Q4 2010 = 130, Q1 2011 = 150, Q4 2011 = 180.
	s := Series{Start: "2010-01", Step: 3, Values: []float64{100, 110, 120, 130, 150, 160, 170, 180}}
	eq(t, "2010-2011", s.Window(2010, 2011), 80)
	eq(t, "2011-2011", s.Window(2011, 2011), 20)
}

func TestYearsCompoundExactlyToTheWindow(t *testing.T) {
	s := monthly()
	years := s.Years(2010, 2012)
	if len(years) != 3 {
		t.Fatalf("years = %v", years)
	}
	// January to January doubles; the last year runs to December.
	if years["2010"] != 100 || years["2011"] != 100 {
		t.Errorf("years = %v, want 100 for 2010 and 2011", years)
	}
	compound := 1.0
	for _, y := range []string{"2010", "2011", "2012"} {
		compound *= 1 + years[y]/100
	}
	// Rounding to one decimal keeps this within a hair of the window.
	if want := 1 + *s.Window(2010, 2012)/100; math.Abs(compound-want) > 0.01 {
		t.Errorf("years compound to %.3f, window is %.3f", compound, want)
	}
}

func TestYearsAnnual(t *testing.T) {
	// A price index: 100, 110, 121 in 2010, 2011, 2012.
	s := Series{Start: "2010-01", Step: 12, Values: []float64{100, 110, 121}}
	years := s.Years(2010, 2012)
	if len(years) != 2 || years["2011"] != 10 || years["2012"] != 10 {
		t.Errorf("years = %v, want 2011 and 2012 at 10", years)
	}
	if _, ok := years["2010"]; ok {
		t.Error("an annual series has no change in its first year")
	}
	eq(t, "annual window", s.Window(2010, 2012), 21)
}

func TestGapsAreNeverFilled(t *testing.T) {
	s := Series{Start: "2010-01", Step: 1, Values: []float64{1, 2}}
	if s.Years(2010, 2011) != nil || s.Window(2010, 2011) != nil {
		t.Error("a series that stops short of the end was served")
	}

	// A series that starts in 2011 (Israel's shekel: the ECB published none in
	// 2010) serves the years it has and not the window that needs 2010.
	v := make([]float64, 36)
	for i := range v {
		v[i] = 100 + float64(i)
	}
	late := Series{Start: "2011-01", Step: 1, Values: v}
	if late.Window(2010, 2013) != nil {
		t.Error("a window starting before the series was served")
	}
	years := late.Years(2010, 2013)
	if _, ok := years["2010"]; ok || len(years) != 3 {
		t.Errorf("years = %v, want 2011, 2012 and 2013 only", years)
	}
	if late.Window(2011, 2013) == nil {
		t.Error("a window the series covers was not served")
	}
	zero := Series{Start: "2010-01", Step: 12, Values: []float64{0, 5}}
	if zero.Window(2010, 2011) != nil {
		t.Error("a window from a zero was served")
	}
}

func TestInvertMakesAStrongerCurrencyPositive(t *testing.T) {
	// The yen goes from 100 to 80 per euro: it bought more euro, so it rose.
	s := Series{Start: "2010-01", Step: 12, Values: []float64{100, 80}}
	eq(t, "local per euro", s.Window(2010, 2011), -20)
	eq(t, "inverted", s.Invert().Window(2010, 2011), 25)
}

func TestNoNegativeZero(t *testing.T) {
	s := Series{Start: "2010-01", Step: 12, Values: []float64{100, 99.99}}
	if v := s.Window(2010, 2011); v == nil || math.Signbit(*v) {
		t.Errorf("window = %v, want a positive zero", v)
	}
}

func TestValidate(t *testing.T) {
	for _, s := range []Series{{Start: "2010-01", Step: 2, Values: []float64{1}}, {Start: "bad", Step: 1, Values: []float64{1}}, {Start: "2010-01", Step: 1}} {
		if s.Validate() == nil {
			t.Errorf("%+v validated", s)
		}
	}
	if err := (Series{Start: "2010-01", Step: 3, Values: []float64{1}}).Validate(); err != nil {
		t.Error(err)
	}
}

func TestEndLabel(t *testing.T) {
	for step, want := range map[int]string{1: "2024-12", 3: "2024-Q4", 12: "2024"} {
		if got := (Series{Start: "2010-01", Step: step}).EndLabel(2024); got != want {
			t.Errorf("step %d: %s, want %s", step, got, want)
		}
	}
}
