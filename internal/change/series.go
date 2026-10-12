// Package change computes the four Atlas "change over a window" measures from a
// stored series (portfolio.md §8; bearing.md #84): shares, housing, prices and
// currency. The fetchers store a series from January 2010 and never a level the
// page could compare across countries; this package turns a series into the
// change over a window and the change within each calendar year, at whatever
// end year the route chooses.
package change

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// FirstYear is the first year every stored series covers.
const FirstYear = 2010

// Series is a contiguous run of values: Values[i] is the observation at
// Start + i*Step months. Step is 1 (monthly), 3 (quarterly) or 12 (annual).
type Series struct {
	Start  string    `json:"start"` // "YYYY-MM"
	Step   int       `json:"step"`
	Values []float64 `json:"values"`
}

func monthIndex(year, month int) int { return year*12 + month - 1 }

func (s Series) startIndex() (int, error) {
	y, m, ok := strings.Cut(s.Start, "-")
	if !ok {
		return 0, fmt.Errorf("change: bad start %q", s.Start)
	}
	yi, err1 := strconv.Atoi(y)
	mi, err2 := strconv.Atoi(m)
	if err1 != nil || err2 != nil || mi < 1 || mi > 12 {
		return 0, fmt.Errorf("change: bad start %q", s.Start)
	}
	return monthIndex(yi, mi), nil
}

// Validate reports whether the series is well formed.
func (s Series) Validate() error {
	if s.Step != 1 && s.Step != 3 && s.Step != 12 {
		return fmt.Errorf("change: step %d, want 1, 3 or 12", s.Step)
	}
	if len(s.Values) == 0 {
		return fmt.Errorf("change: empty series")
	}
	_, err := s.startIndex()
	return err
}

// At returns the observation for the given month, if the series has one.
func (s Series) At(year, month int) (float64, bool) {
	start, err := s.startIndex()
	if err != nil || s.Step < 1 {
		return 0, false
	}
	d := monthIndex(year, month) - start
	if d < 0 || d%s.Step != 0 || d/s.Step >= len(s.Values) {
		return 0, false
	}
	return s.Values[d/s.Step], true
}

// Last is the year and month of the final observation.
func (s Series) Last() (year, month int) {
	start, err := s.startIndex()
	if err != nil || len(s.Values) == 0 {
		return 0, 0
	}
	i := start + (len(s.Values)-1)*s.Step
	return i / 12, i%12 + 1
}

// AsOf labels the final observation: "2024-12", "2024-Q4" or "2024".
func (s Series) AsOf() string {
	y, m := s.Last()
	switch s.Step {
	case 3:
		return fmt.Sprintf("%d-Q%d", y, (m-1)/3+1)
	case 12:
		return strconv.Itoa(y)
	}
	return fmt.Sprintf("%d-%02d", y, m)
}

// endMonth is the month whose observation closes a year: December for a
// monthly series, the fourth-quarter observation for a quarterly one, and the
// year's single value for an annual one.
func (s Series) endMonth() int {
	switch s.Step {
	case 3:
		return 10
	case 12:
		return 1
	}
	return 12
}

// LastCompleteYear is the latest year whose closing observation exists, so a
// monthly series ending in August 2026 answers 2025.
func (s Series) LastCompleteYear() int {
	y, m := s.Last()
	if m >= s.endMonth() {
		return y
	}
	return y - 1
}

// EndLabel names the closing observation of a year, the point a window ends at:
// "2024-12", "2024-Q4" or "2024".
func (s Series) EndLabel(year int) string {
	switch s.Step {
	case 3:
		return fmt.Sprintf("%d-Q4", year)
	case 12:
		return strconv.Itoa(year)
	}
	return fmt.Sprintf("%d-12", year)
}

// Invert returns 1/v for every value. The ECB quotes local currency per €1, so
// the inverse is the euro value of the local currency, and a rise in it means
// the local currency strengthened.
func (s Series) Invert() Series {
	out := Series{Start: s.Start, Step: s.Step, Values: make([]float64, len(s.Values))}
	for i, v := range s.Values {
		out.Values[i] = 1 / v
	}
	return out
}

func pct(from, to float64) (float64, bool) {
	if from <= 0 || to <= 0 {
		return 0, false
	}
	v := math.Round((to/from-1)*1000) / 10 // percent, one decimal
	if v == 0 {
		v = 0 // no negative zero
	}
	return v, true
}

// Window is the change from January of startYear to the end of endYear, in
// percent. Taking a ratio between two points of one published series is not
// re-basing. It is nil when either point is missing: a window is never filled
// with a stand-in.
func (s Series) Window(startYear, endYear int) *float64 {
	from, ok1 := s.At(startYear, 1)
	to, ok2 := s.At(endYear, s.endMonth())
	if !ok1 || !ok2 {
		return nil
	}
	v, ok := pct(from, to)
	if !ok {
		return nil
	}
	return &v
}

// Years is the change within each calendar year from firstYear to endYear.
// For a monthly or quarterly series a year runs from its first observation to
// the first of the next, and the last year to the end month, so the years
// compound exactly to Window(firstYear, endYear). For an annual series each
// year is its value against the year before, starting the year after the
// first. A year with a missing point is left out, never filled; the result is
// nil when no year can be served.
func (s Series) Years(firstYear, endYear int) map[string]float64 {
	out := map[string]float64{}
	if s.Step == 12 {
		for y := firstYear + 1; y <= endYear; y++ {
			prev, ok1 := s.At(y-1, 1)
			cur, ok2 := s.At(y, 1)
			if v, ok := pct(prev, cur); ok1 && ok2 && ok {
				out[strconv.Itoa(y)] = v
			}
		}
		return nilIfEmpty(out)
	}
	for y := firstYear; y <= endYear; y++ {
		from, ok1 := s.At(y, 1)
		var to float64
		var ok2 bool
		if y < endYear {
			to, ok2 = s.At(y+1, 1)
		} else {
			to, ok2 = s.At(endYear, s.endMonth())
		}
		if v, ok := pct(from, to); ok1 && ok2 && ok {
			out[strconv.Itoa(y)] = v
		}
	}
	return nilIfEmpty(out)
}

func nilIfEmpty(m map[string]float64) map[string]float64 {
	if len(m) == 0 {
		return nil
	}
	return m
}
