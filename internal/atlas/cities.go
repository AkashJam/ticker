package atlas

import (
	"fmt"
	"strconv"
	"time"

	"github.com/AkashJam/ticker/internal/change"
	"github.com/AkashJam/ticker/internal/ecb"
	"github.com/AkashJam/ticker/internal/fred"
	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/worldbank"
)

// Windows are the three window starts, longest last. Every change measure is
// reported from each of them to the common end.
var Windows = []string{"2021", "2016", "2010"}

// WindowChange is one of the four change measures (shares, housing, prices,
// currency). It carries change over each window and within each calendar year,
// never a level, because each country's index has its own base (bearing.md
// "Indices are change, never levels").
//
// Change is null as a whole when the measure cannot be served for the country
// (Reason "euro" or "stale"); an individual window is null when its start point
// was never published (Reason "unpublished": Israel's 2010, bearing.md #99).
type WindowChange struct {
	Change  map[string]*float64 `json:"change"`
	Years   map[string]float64  `json:"years"`
	Reason  string              `json:"reason,omitempty"`  // euro | stale | unpublished
	Derived string              `json:"derived,omitempty"` // "via USD" for Chile
	Source  string              `json:"source"`
	AsOf    string              `json:"asOf"`
}

// Sourced is one level measure: the latest published value, with its source
// and as-of. A value that cannot be served is null with a Reason.
type Sourced struct {
	Value  *float64 `json:"value"`
	Unit   string   `json:"unit,omitempty"`
	Source string   `json:"source"`
	AsOf   string   `json:"asOf"`
	Reason string   `json:"reason,omitempty"`
}

// Row is one country, labelled by city. Eight measures: four change over one
// window, four the latest level. Cost of living and the one-year inflation rate
// are city-page figures and are not here.
type Row struct {
	Slug        string  `json:"slug"`
	City        string  `json:"city"`
	Country     string  `json:"country"`
	Code        string  `json:"code"`
	Sensor      *string `json:"sensor"`
	HasCityPage bool    `json:"hasCityPage"`
	Region      string  `json:"region"`

	Shares   WindowChange `json:"shares"`
	Housing  WindowChange `json:"housing"`
	Prices   WindowChange `json:"prices"`
	Currency WindowChange `json:"currency"`

	EverydaySpendingEur Sourced `json:"everydaySpendingEur"`
	GniPpp              Sourced `json:"gniPpp"`
	Unemployment        Sourced `json:"unemployment"`
	LifeExpectancy      Sourced `json:"lifeExpectancy"`
}

// Response is GET /atlas/cities (portfolio.md §8).
type Response struct {
	GeneratedAt string   `json:"generatedAt"`
	Windows     []string `json:"windows"`
	End         string   `json:"end"`
	Rows        []Row    `json:"rows"`
}

// Build assembles the response from the three stored datasets. countries must
// be the Atlas rows; they set the order. Every change measure is cut at the
// common end year, so one window applies to all of them.
func Build(countries []reference.Country, f fred.Dataset, e ecb.Dataset, w worldbank.Dataset) (Response, error) {
	end, err := CommonEnd(countries, f, e, w)
	if err != nil {
		return Response{}, err
	}
	resp := Response{
		GeneratedAt: latest(f.GeneratedAt, e.GeneratedAt, w.GeneratedAt),
		Windows:     Windows,
		End:         strconv.Itoa(end),
		Rows:        make([]Row, 0, len(countries)),
	}
	for _, c := range countries {
		fr, wr := f.Rows[c.Code], w.Rows[c.Code]
		city := ""
		if c.City != nil {
			city = *c.City
		}
		resp.Rows = append(resp.Rows, Row{
			Slug: c.Slug, City: city, Country: c.Country, Code: c.Code,
			Sensor: c.Sensor, HasCityPage: c.HasCityPage, Region: c.Region,

			Shares:   measure(fr.Shares.Series, fr.Shares.Source, end),
			Housing:  measure(fr.Housing.Series, fr.Housing.Source, end),
			Prices:   measure(wr.Prices.Series, wr.Prices.Source, end),
			Currency: currency(c, e, end),

			EverydaySpendingEur: sourced(wr.EverydaySpendingEur),
			GniPpp:              sourced(wr.GniPpp),
			Unemployment:        sourced(wr.Unemployment),
			LifeExpectancy:      sourced(wr.LifeExpectancy),
		})
	}
	return resp, nil
}

// measure is change over each window and within each year, to end. A window
// whose start point does not exist is null, and the measure says why.
func measure(s change.Series, source string, end int) WindowChange {
	wc := WindowChange{Source: source, AsOf: s.EndLabel(end), Change: make(map[string]*float64, len(Windows))}
	for _, w := range Windows {
		start, _ := strconv.Atoi(w)
		v := s.Window(start, end)
		wc.Change[w] = v
		if v == nil {
			wc.Reason = "unpublished"
		}
	}
	wc.Years = s.Years(change.FirstYear, end)
	return wc
}

// currency is the change in a country's currency against the euro. The ECB
// quotes local units per €1, so the series is inverted first: a positive
// change means the local currency strengthened.
func currency(c reference.Country, e ecb.Dataset, end int) WindowChange {
	asOf := fmt.Sprintf("%d-12", end)
	if c.Currency == nil { // the euro has no rate against itself
		return WindowChange{Reason: "euro", Source: ecb.Source, AsOf: asOf}
	}
	cur := e.Currencies[*c.Currency]
	if cur.Series == nil {
		return WindowChange{Reason: cur.Reason, Derived: cur.Derived, Source: cur.Source, AsOf: asOf}
	}
	wc := measure(cur.Series.Invert(), cur.Source, end)
	wc.Derived = cur.Derived
	return wc
}

func sourced(v worldbank.Value) Sourced {
	return Sourced{Value: v.Value, Unit: v.Unit, Source: v.Source, AsOf: v.AsOf, Reason: v.Reason}
}

// latest is the most recent of the datasets' generation times, so a response
// says how fresh its newest ingredient is.
func latest(times ...string) string {
	best, bestT := "", time.Time{}
	for _, s := range times {
		if t, err := time.Parse(time.RFC3339, s); err == nil && t.After(bestT) {
			best, bestT = s, t
		}
	}
	return best
}
