package ecb

import (
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/change"
)

func TestComputeHistoryRunsFrom1999WhileComputeKeepsTo2010(t *testing.T) {
	st := NewState([]string{"JPY"})
	// One rate on the first of every month from January 1999 to March 2011, then a
	// last day so the final month is complete.
	var days []Day
	for d := time.Date(1999, 1, 4, 0, 0, 0, 0, time.UTC); d.Before(time.Date(2011, 4, 1, 0, 0, 0, 0, time.UTC)); d = d.AddDate(0, 0, 1) {
		days = append(days, Day{Date: d, Rates: map[string]float64{"JPY": 100}})
	}
	st.Append(days)
	now := time.Date(2011, 4, 5, 0, 0, 0, 0, time.UTC)

	h := st.ComputeHistory(now).Currencies["JPY"]
	if h.Series == nil || h.Series.Start != "1999-01" {
		t.Fatalf("history starts %+v", h.Series)
	}
	n := st.Compute(now).Currencies["JPY"]
	if n.Series == nil || n.Series.Start != "2010-01" || n.Series.Start == h.Series.Start {
		t.Fatalf("the /atlas series moved: %+v", n.Series)
	}
}

func TestTrimToStartsAtTheOtherSeriesFirstMonth(t *testing.T) {
	pesos := change.Series{Start: "1998-11", Step: 1, Values: []float64{1, 2, 3, 4}}
	usd := change.Series{Start: "1999-01", Step: 1, Values: []float64{9}}
	got := TrimTo(pesos, usd)
	if got.Start != "1999-01" || len(got.Values) != 2 || got.Values[0] != 3 {
		t.Errorf("%+v", got)
	}
	if same := TrimTo(usd, pesos); same.Start != "1999-01" {
		t.Errorf("a series that starts later was cut: %+v", same)
	}
}
