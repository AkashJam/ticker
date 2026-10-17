package trigger

import (
	"errors"
	"time"
)

// The expectation band (phase15.md step 7, decision 2): where a sensor
// "normally trades given what its sector did that day", drawn in price terms.
// For each session the edges are
//
//	previous close x (1 + the benchmark's return that day +- threshold x sigma)
//
// with the event's own sigma and threshold, so the line leaves the band on
// exactly the session the residual passes the threshold, and every edge can be
// reproduced from the figures the entry states and the daily candles. The band
// drifts with the sector, which is why it is not flat. A sensor with no
// benchmark (AAPL) is centred on the previous close.

// BandWindow is how many sessions the band covers: the sixty the sigma was
// measured over.
const BandWindow = 60

// Band is a sensor's closes with the band around them, oldest first. The slices
// are the same length.
type Band struct {
	Dates []time.Time `json:"dates"`
	Close []float64   `json:"close"`
	Lower []float64   `json:"lower"`
	Upper []float64   `json:"upper"`
}

// ErrNoBand means the closes held no session with a valid return.
var ErrNoBand = errors.New("trigger: no sessions to draw a band over")

// BuildBand returns the band over the BandWindow most recent sessions up to and
// including sessions[at] that have a valid return (the symbol's, and its
// benchmark's where it has one). bench is nil for a sensor with no benchmark.
func BuildBand(sessions []time.Time, at int, sym, bench Closes, sigma, threshold float64) (Band, error) {
	if at < 1 || at >= len(sessions) {
		return Band{}, ErrNoBand
	}
	var rev Band
	for i := at; i >= 1 && len(rev.Dates) < BandWindow; i-- {
		pt, ok := pointAt(sessions, i, sym, bench)
		if !ok {
			continue
		}
		prev := sym[sessions[i-1]]
		var b float64
		if pt.Benchmark != nil {
			b = *pt.Benchmark
		}
		rev.Dates = append(rev.Dates, sessions[i])
		rev.Close = append(rev.Close, sym[sessions[i]])
		rev.Lower = append(rev.Lower, prev*(1+b-threshold*sigma))
		rev.Upper = append(rev.Upper, prev*(1+b+threshold*sigma))
	}
	if len(rev.Dates) == 0 {
		return Band{}, ErrNoBand
	}
	n := len(rev.Dates)
	out := Band{Dates: make([]time.Time, n), Close: make([]float64, n), Lower: make([]float64, n), Upper: make([]float64, n)}
	for i := range n {
		j := n - 1 - i
		out.Dates[i], out.Close[i], out.Lower[i], out.Upper[i] = rev.Dates[j], rev.Close[j], rev.Lower[j], rev.Upper[j]
	}
	return out, nil
}
