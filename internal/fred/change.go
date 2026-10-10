package fred

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// Freq is a series' publication frequency.
type Freq int

const (
	Monthly Freq = iota
	Quarterly
)

// Quarter is a calendar quarter, e.g. 2025-Q4.
type Quarter struct{ Year, Q int }

func (q Quarter) String() string { return fmt.Sprintf("%d-Q%d", q.Year, q.Q) }

func (q Quarter) index() int { return q.Year*4 + q.Q - 1 }

// Before reports whether q is earlier than o.
func (q Quarter) Before(o Quarter) bool { return q.index() < o.index() }

// LastCompleteQuarter is the latest quarter the series covers in full. A
// monthly series ending in August has not finished Q3, so its answer is Q2; a
// quarterly series is dated at the quarter's first day, so its last
// observation's quarter is already complete.
func LastCompleteQuarter(obs []Obs, f Freq) Quarter {
	last := obs[len(obs)-1].Date
	q := Quarter{last.Year(), (int(last.Month())-1)/3 + 1}
	if f == Monthly && int(last.Month())%3 != 0 {
		if q.Q == 1 {
			return Quarter{q.Year - 1, 4}
		}
		return Quarter{q.Year, q.Q - 1}
	}
	return q
}

// CommonEnd is the earliest of the given quarters: the latest quarter every
// country has, so every row is measured to the same end. Japan's BIS series
// runs one quarter behind the rest and sets it.
func CommonEnd(qs []Quarter) Quarter {
	end := qs[0]
	for _, q := range qs[1:] {
		if q.Before(end) {
			end = q
		}
	}
	return end
}

func index(obs []Obs) map[time.Time]float64 {
	m := make(map[time.Time]float64, len(obs))
	for _, o := range obs {
		m[o.Date] = o.Value
	}
	return m
}

func date(y, m int) time.Time { return time.Date(y, time.Month(m), 1, 0, 0, 0, 0, time.UTC) }

// startPoint is the January observation of year: the first of the year for
// both frequencies, since a quarterly series is dated at its quarter's first
// day.
func startPoint(m map[time.Time]float64, year int) (float64, bool) {
	v, ok := m[date(year, 1)]
	return v, ok
}

// endPoint is the observation for the last month of q (monthly) or q itself
// (quarterly).
func endPoint(m map[time.Time]float64, f Freq, q Quarter) (float64, bool) {
	month := q.Q*3 - 2
	if f == Monthly {
		month = q.Q * 3
	}
	v, ok := m[date(q.Year, month)]
	return v, ok
}

func pct(from, to float64) *float64 {
	if from == 0 {
		return nil
	}
	v := math.Round((to/from-1)*1000) / 10 // percent, one decimal
	return &v
}

// TrailPoint is the change since the 2010 origin at a waypoint, which is a
// year, or the common end quarter for the last point.
type TrailPoint struct {
	At     string   `json:"at"`
	Change *float64 `json:"change"`
}

// Window returns the change from January of startYear to the end of q. It is
// nil when either point is missing: a window is never filled with a stand-in.
func Window(obs []Obs, f Freq, startYear int, q Quarter) *float64 {
	m := index(obs)
	from, ok1 := startPoint(m, startYear)
	to, ok2 := endPoint(m, f, q)
	if !ok1 || !ok2 {
		return nil
	}
	return pct(from, to)
}

// Trail returns the change since January 2010 at each waypoint year and at the
// end quarter. Every path starts at the common 2010 origin because both axes
// are change.
func Trail(obs []Obs, f Freq, waypoints []int, q Quarter) []TrailPoint {
	m := index(obs)
	origin, haveOrigin := startPoint(m, 2010)
	out := make([]TrailPoint, 0, len(waypoints)+1)
	for _, y := range waypoints {
		p := TrailPoint{At: strconv.Itoa(y)}
		if v, ok := startPoint(m, y); ok && haveOrigin {
			p.Change = pct(origin, v)
		}
		out = append(out, p)
	}
	end := TrailPoint{At: q.String()}
	if v, ok := endPoint(m, f, q); ok && haveOrigin {
		end.Change = pct(origin, v)
	}
	return append(out, end)
}
