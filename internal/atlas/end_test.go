package atlas

import (
	"strings"
	"testing"

	"github.com/AkashJam/ticker/internal/change"
	"github.com/AkashJam/ticker/internal/ecb"
	"github.com/AkashJam/ticker/internal/fred"
	"github.com/AkashJam/ticker/internal/worldbank"
)

func str(s string) *string { return &s }

// monthly runs from 2010-01 for n months; quarterly for n quarters; annual n years.
func monthly(n int) change.Series {
	return change.Series{Start: "2010-01", Step: 1, Values: make([]float64, n)}
}
func quarterly(n int) change.Series {
	return change.Series{Start: "2010-01", Step: 3, Values: make([]float64, n)}
}
func annualTo(year int) change.Series {
	return change.Series{Start: "2010-01", Step: 12, Values: make([]float64, year-2009)}
}

// fixture: Italy (euro) and the US, both complete to 2025 except the US CPI.
func fixture() ([]Country, fred.Dataset, ecb.Dataset, worldbank.Dataset) {
	cs := []Country{{"IT", nil}, {"US", str("USD")}}
	row := fred.Row{
		Shares:  fred.Measure{Series: monthly(12*16 + 8)},  // to 2026-08
		Housing: fred.Measure{Series: quarterly(4*16 + 1)}, // to 2026-Q1
	}
	f := fred.Dataset{Rows: map[string]fred.Row{"IT": row, "US": row}}
	usd := monthly(12*16 + 9)
	e := ecb.Dataset{Currencies: map[string]ecb.Currency{"USD": {Series: &usd}}}
	w := worldbank.Dataset{Rows: map[string]worldbank.Row{
		"IT": {Prices: worldbank.Prices{Series: annualTo(2025)}},
		"US": {Prices: worldbank.Prices{Series: annualTo(2024)}}, // no 2025 CPI yet
	}}
	return cs, f, e, w
}

func TestCommonEndIsBoundByTheLaggingMeasure(t *testing.T) {
	cs, f, e, w := fixture()
	if got, err := CommonEnd(cs, f, e, w); err != nil || got != 2024 {
		t.Errorf("end = %d err = %v, want 2024 (the US has no 2025 CPI)", got, err)
	}

	// The end moves on by itself once the last country publishes.
	w.Rows["US"] = worldbank.Row{Prices: worldbank.Prices{Series: annualTo(2025)}}
	if got, _ := CommonEnd(cs, f, e, w); got != 2025 {
		t.Errorf("end = %d, want 2025 once the US CPI arrives", got)
	}
}

func TestIncompleteYearsDoNotCount(t *testing.T) {
	cs, f, e, w := fixture()
	w.Rows["US"] = worldbank.Row{Prices: worldbank.Prices{Series: annualTo(2025)}}
	// Housing ending in Q3 2025 has not reached the Q4 observation that closes 2025.
	r := f.Rows["US"]
	r.Housing = fred.Measure{Series: quarterly(4*15 + 3)}
	f.Rows["US"] = r
	if got, _ := CommonEnd(cs, f, e, w); got != 2024 {
		t.Errorf("end = %d, want 2024", got)
	}
}

func TestEuroRowsAndStaleChileAreSkipped(t *testing.T) {
	cs, f, e, w := fixture()
	cs = append(cs, Country{"CL", str("CLP")})
	f.Rows["CL"], w.Rows["CL"] = f.Rows["US"], w.Rows["US"]
	e.Currencies["CLP"] = ecb.Currency{Reason: "stale"}
	if got, err := CommonEnd(cs, f, e, w); err != nil || got != 2024 {
		t.Errorf("end = %d err = %v: a stale Chile is a stated gap, not a limit on the page", got, err)
	}
}

func TestCommonEndRefusesToGuess(t *testing.T) {
	cs, f, e, w := fixture()
	e.Currencies["USD"] = ecb.Currency{Reason: "not published by the ECB"}
	if _, err := CommonEnd(cs, f, e, w); err == nil || !strings.Contains(err.Error(), "US") {
		t.Errorf("err = %v, want the US named", err)
	}

	cs, f, e, w = fixture()
	delete(w.Rows, "IT")
	if _, err := CommonEnd(cs, f, e, w); err == nil {
		t.Error("a country missing from a dataset was ignored")
	}
	if _, err := CommonEnd(nil, f, e, w); err == nil {
		t.Error("no countries was accepted")
	}
}
