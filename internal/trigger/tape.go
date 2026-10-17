package trigger

import (
	"slices"
	"time"
)

// TapeWindow is the sessions of closes a sparkline draws.
const TapeWindow = 60

// TapeSensor is one sensor's row of the tape's two history columns: a
// sparkline of its last sixty closes and its z on the last close
// ("sigma, last close"). The z is the trigger's own, the residual against the
// sensor's benchmark over the sixty sessions before it (bearing.md #92), so the
// column reads the number that would have fired. Z is nil, with a Reason, where
// the trigger could not judge the sensor.
type TapeSensor struct {
	Symbol string      `json:"symbol"`
	Dates  []time.Time `json:"dates"`
	Closes []float64   `json:"closes"`
	Z      *float64    `json:"z"`
	Sigma  *float64    `json:"sigma"`
	Reason string      `json:"reason,omitempty"`
}

// Tape is the page's history columns for every sensor.
type Tape struct {
	AsOf    time.Time    `json:"asOf"` // the newest session in the data
	Sensors []TapeSensor `json:"sensors"`
}

// BuildTape computes the tape for sensors from daily closes (the sensors and
// their benchmarks, as trigger.Load returns them).
func BuildTape(all map[string]Closes, sensors []string) Tape {
	sessions := Sessions(all)
	t := Tape{Sensors: make([]TapeSensor, 0, len(sensors))}
	if len(sessions) == 0 {
		for _, s := range sensors {
			t.Sensors = append(t.Sensors, TapeSensor{Symbol: s, Dates: []time.Time{}, Closes: []float64{}, Reason: ReasonOf(ErrNoSession)})
		}
		return t
	}
	t.AsOf = sessions[len(sessions)-1]
	results, errs := EvaluateOn(all, sensors, t.AsOf)
	byz := map[string]Result{}
	for _, r := range results {
		byz[r.Symbol] = r
	}
	for _, s := range sensors {
		row := TapeSensor{Symbol: s}
		row.Dates, row.Closes = lastCloses(all[s], TapeWindow)
		if r, ok := byz[s]; ok {
			z, sigma := r.Z, r.Sigma
			row.Z, row.Sigma = &z, &sigma
		} else {
			row.Reason = ReasonOf(errs[s])
		}
		t.Sensors = append(t.Sensors, row)
	}
	return t
}

// lastCloses returns up to n closes, oldest first, with their dates.
func lastCloses(c Closes, n int) ([]time.Time, []float64) {
	dates := make([]time.Time, 0, len(c))
	for d := range c {
		dates = append(dates, d)
	}
	slices.SortFunc(dates, func(a, b time.Time) int { return a.Compare(b) })
	if len(dates) > n {
		dates = dates[len(dates)-n:]
	}
	closes := make([]float64, len(dates))
	for i, d := range dates {
		closes[i] = c[d]
	}
	return dates, closes
}
