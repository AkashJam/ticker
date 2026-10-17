package fred

import (
	"testing"
	"time"
)

func obsFrom(start time.Time, step int, vals ...float64) []Obs {
	var out []Obs
	for i, v := range vals {
		out = append(out, Obs{Date: start.AddDate(0, i*step, 0), Value: v})
	}
	return out
}

func TestToHistoryStartsAtTheFirstObservation(t *testing.T) {
	s, err := ToHistory(obsFrom(time.Date(1957, 12, 1, 0, 0, 0, 0, time.UTC), 1, 1, 2, 3), 1)
	if err != nil || s.Start != "1957-12" || len(s.Values) != 3 || s.AsOf() != "1958-02" {
		t.Fatalf("%+v %v", s, err)
	}
	q, err := ToHistory(obsFrom(time.Date(1955, 1, 1, 0, 0, 0, 0, time.UTC), 3, 10, 11), 3)
	if err != nil || q.Start != "1955-01" || q.Step != 3 {
		t.Fatalf("%+v %v", q, err)
	}
}

func TestToHistoryRefusesAGapAndAnEmptySeries(t *testing.T) {
	obs := obsFrom(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), 1, 1, 2, 3)
	obs[2].Date = obs[2].Date.AddDate(0, 1, 0) // skips a month
	if _, err := ToHistory(obs, 1); err == nil {
		t.Error("a skipped month was accepted")
	}
	if _, err := ToHistory(nil, 1); err == nil {
		t.Error("an empty series was accepted")
	}
}
