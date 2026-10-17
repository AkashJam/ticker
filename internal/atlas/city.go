package atlas

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/AkashJam/ticker/internal/change"
	"github.com/AkashJam/ticker/internal/ecb"
	"github.com/AkashJam/ticker/internal/fred"
	"github.com/AkashJam/ticker/internal/history"
	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/worldbank"
)

// ErrNoCity means there is no city record for the slug: it is not an Atlas
// country with a survey.
var ErrNoCity = errors.New("atlas: no such city")

// CityResponse is GET /atlas/cities/{city} (portfolio.md §8; Phase 16 step 3):
// everything one city page draws. Every derived figure carries its legs and the
// date of each, so the page can print one without computing it.
type CityResponse struct {
	GeneratedAt string  `json:"generatedAt"`
	Slug        string  `json:"slug"`
	City        string  `json:"city"`
	Country     string  `json:"country"`
	Code        string  `json:"code"`
	Sensor      *string `json:"sensor"`
	Region      string  `json:"region"`
	// Verified is false while an item the owner must close is open; the page
	// ships only for a verified city, and hasCityPage flips with it.
	Verified bool `json:"verified"`

	Survey      CitySurvey      `json:"survey"`
	Cost        CityCost        `json:"cost"`
	Charts      CityCharts      `json:"charts"`
	Macro       CityMacro       `json:"macro"`
	Composition CityComposition `json:"composition"`
}

// CitySurvey is the survey record as the page draws it, plus the euro value of
// each figure beside it.
type CitySurvey struct {
	Instrument  string                   `json:"instrument"`
	Publisher   string                   `json:"publisher"`
	Headline    reference.SurveyHeadline `json:"headline"`
	SecondLines []CitySecondLine         `json:"secondLines"`
	Housing     reference.SurveyHousing  `json:"housing"`
	Parts       []reference.SurveyPart   `json:"parts"`
	SplitNote   *string                  `json:"splitNote"`
	Sources     []reference.SurveySource `json:"sources"`
	NextRelease *string                  `json:"nextRelease"`
	OpenItems   []string                 `json:"openItems"`
}

// CitySecondLine is a labelled figure beside the headline, with its euro value
// at the same rate as the headline's.
type CitySecondLine struct {
	Label string   `json:"label"`
	Value float64  `json:"value"`
	Eur   *float64 `json:"eur"`
}

// CPILeg is one end of the carry-forward: the World Bank CPI index in a year.
type CPILeg struct {
	Year  int     `json:"year"`
	Index float64 `json:"index"`
}

// Carried is the headline carried forward on the country's CPI to the latest
// published year and no further, stated in that year's prices (bearing.md #98).
// Value is null, with a Reason, when there is nothing to carry: the survey is as
// new as the CPI or newer, or the CPI cannot be read.
type Carried struct {
	Value  *float64 `json:"value"`
	Year   int      `json:"year"` // the price year the figure is stated in
	From   *CPILeg  `json:"from"` // CPI in the survey's year
	To     *CPILeg  `json:"to"`   // CPI in the latest published year
	Factor *float64 `json:"factor"`
	Source string   `json:"source"`
	Reason string   `json:"reason,omitempty"`
}

// Euro is the conversion to euros at the ECB's annual average, a dated reference
// rate and never a frozen constant.
type Euro struct {
	Value  *float64 `json:"value"` // the figure shown, in euros
	Rate   *float64 `json:"rate"`  // local units per €1
	Year   int      `json:"year"`  // the year the rate averages
	Source string   `json:"source"`
	Reason string   `json:"reason,omitempty"` // euro | not published by the ECB
	// Basis says which figure was converted: the carried one, or the survey's own.
	Basis string `json:"basis"`
}

// CityCost is the derived cost-of-living block.
type CityCost struct {
	Carried Carried `json:"carried"`
	Euro    Euro    `json:"eur"`
}

// Chart is one long series with where it came from. Series is null, with a
// Reason, when it cannot be served.
type Chart struct {
	Series  *change.Series `json:"series"`
	Source  string         `json:"source"`
	AsOf    string         `json:"asOf"`
	Derived string         `json:"derived,omitempty"`
	Reason  string         `json:"reason,omitempty"`
	// Behind is how many months this series ends before the newest series of
	// its kind among all countries; /atlas ends every change measure at one year
	// so the lag never shows there, and the page says so beside the chart.
	Behind int `json:"behind"`
}

// CityCharts are the three long charts: the OECD share index, the currency
// against the euro (local units per €1, as the ECB quotes it) and BIS real
// house prices.
type CityCharts struct {
	Shares   Chart `json:"shares"`
	Currency Chart `json:"currency"`
	Housing  Chart `json:"housing"`
}

// CityMacro is the five macro series: four levels and the one-year inflation
// rate, each with its source and as-of.
type CityMacro struct {
	EverydaySpendingEur Sourced `json:"everydaySpendingEur"`
	GniPpp              Sourced `json:"gniPpp"`
	Unemployment        Sourced `json:"unemployment"`
	LifeExpectancy      Sourced `json:"lifeExpectancy"`
	Inflation           Sourced `json:"inflation"`
}

// CityComposition is what the economy is made of, as shares of GDP. Residual is
// derived: taxes less subsidies, 100 less the three sectors.
type CityComposition struct {
	Agriculture   Sourced `json:"agriculture"`
	Industry      Sourced `json:"industry"`
	Services      Sourced `json:"services"`
	Manufacturing Sourced `json:"manufacturing"` // part of industry
	Residual      Sourced `json:"residual"`
}

// CityInput is the stored data BuildCity reads.
type CityInput struct {
	Countries []reference.Country // the Atlas rows
	Surveys   []reference.Survey
	History   history.Dataset
	ECB       ecb.Dataset // the daily-averaged rates, for the annual averages
	WB        worldbank.Dataset
}

// BuildCity assembles one city's page data. It errors with ErrNoCity for a slug
// that is not an Atlas country with a survey.
func BuildCity(slug string, in CityInput) (CityResponse, error) {
	var country *reference.Country
	for i := range in.Countries {
		if in.Countries[i].Slug == slug {
			country = &in.Countries[i]
		}
	}
	var sv *reference.Survey
	for i := range in.Surveys {
		if in.Surveys[i].Slug == slug {
			sv = &in.Surveys[i]
		}
	}
	if country == nil || sv == nil || country.City == nil {
		return CityResponse{}, ErrNoCity
	}
	wb := in.WB.Rows[country.Code]
	hist := in.History.Rows[country.Code]

	cost := buildCost(*country, *sv, wb, in.ECB)
	rate := cost.Euro.Rate
	toEur := func(v float64) *float64 {
		if cost.Euro.Reason == "euro" {
			r := round2(v)
			return &r
		}
		if rate == nil || *rate == 0 {
			return nil
		}
		r := round2(v / *rate)
		return &r
	}

	resp := CityResponse{
		GeneratedAt: latest(in.History.GeneratedAt, in.ECB.GeneratedAt, in.WB.GeneratedAt),
		Slug:        country.Slug, City: *country.City, Country: country.Country, Code: country.Code,
		Sensor: country.Sensor, Region: country.Region, Verified: sv.Verified,
		Survey: CitySurvey{
			Instrument: sv.Instrument, Publisher: sv.Publisher, Headline: sv.Headline,
			Housing: sv.Housing, Parts: sv.Parts, SplitNote: sv.SplitNote, Sources: sv.Sources,
			NextRelease: sv.NextRelease, OpenItems: nonNil(sv.OpenItems),
		},
		Cost: cost,
		Charts: CityCharts{
			Shares:   chartOf(hist.Shares, in.History, func(r history.Row) fred.Measure { return r.Shares }),
			Currency: currencyChart(*country, in.History),
			Housing:  chartOf(hist.Housing, in.History, func(r history.Row) fred.Measure { return r.Housing }),
		},
		Macro: CityMacro{
			EverydaySpendingEur: sourced(wb.EverydaySpendingEur), GniPpp: sourced(wb.GniPpp),
			Unemployment: sourced(wb.Unemployment), LifeExpectancy: sourced(wb.LifeExpectancy),
			Inflation: sourced(wb.Inflation),
		},
		Composition: composition(wb.Composition),
	}
	resp.Survey.SecondLines = make([]CitySecondLine, 0, len(sv.SecondLines))
	for _, l := range sv.SecondLines {
		resp.Survey.SecondLines = append(resp.Survey.SecondLines, CitySecondLine{Label: l.Label, Value: l.Value, Eur: toEur(l.Value)})
	}
	return resp, nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }

// buildCost carries the headline on CPI and converts it to euros.
func buildCost(c reference.Country, sv reference.Survey, wb worldbank.Row, e ecb.Dataset) CityCost {
	h := sv.Headline
	carried := Carried{Year: h.Year, Source: wb.Prices.Source}
	cpiYear, _ := wb.Prices.Series.Last()
	from, okFrom := wb.Prices.Series.At(h.Year, 1)
	to, okTo := wb.Prices.Series.At(cpiYear, 1)
	switch {
	case wb.Prices.Series.Validate() != nil || !okTo:
		carried.Reason = "no CPI series"
	case h.Year >= cpiYear:
		// Newer than the latest CPI year, or the same: carrying would invent a
		// year of inflation the World Bank has not published.
		carried.To = &CPILeg{Year: cpiYear, Index: round2(to)}
		carried.Reason = fmt.Sprintf("the survey (%d) is not older than the latest CPI year (%d)", h.Year, cpiYear)
	case !okFrom:
		carried.Reason = fmt.Sprintf("no CPI for %d", h.Year)
	default:
		f := to / from
		v := round2(h.Value * f)
		fr := math.Round(f*10000) / 10000
		carried.Value, carried.Year, carried.Factor = &v, cpiYear, &fr
		carried.From = &CPILeg{Year: h.Year, Index: round2(from)}
		carried.To = &CPILeg{Year: cpiYear, Index: round2(to)}
	}

	// The figure shown in euros is the carried one when there is one.
	shown, shownYear, basis := h.Value, h.Year, "survey"
	if carried.Value != nil {
		shown, shownYear, basis = *carried.Value, carried.Year, "carried"
	}
	eur := Euro{Source: ecb.Source, Basis: basis}
	if c.Currency == nil {
		v := round2(shown)
		eur.Value, eur.Reason = &v, "euro"
		return CityCost{Carried: carried, Euro: eur}
	}
	cur := e.Currencies[*c.Currency]
	if cur.Series == nil {
		eur.Reason = firstNonEmpty(cur.Reason, "not published by the ECB")
		return CityCost{Carried: carried, Euro: eur}
	}
	// The latest complete year no later than the figure's: a partial year's
	// average is not a reference rate.
	lastComplete := cur.Series.LastCompleteYear()
	year := min(shownYear, lastComplete)
	rate, ok := cur.AnnualAverage[strconv.Itoa(year)]
	if !ok || rate == 0 {
		eur.Reason = fmt.Sprintf("no ECB average for %d", year)
		return CityCost{Carried: carried, Euro: eur}
	}
	v := round2(shown / rate)
	eur.Value, eur.Rate, eur.Year = &v, &rate, year
	if cur.Derived != "" {
		eur.Source = cur.Source
	}
	return CityCost{Carried: carried, Euro: eur}
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// chartOf is a country's long series with how far behind the newest country's
// it ends.
func chartOf(m fred.Measure, ds history.Dataset, pick func(history.Row) fred.Measure) Chart {
	if m.Series.Validate() != nil {
		return Chart{Source: m.Source, AsOf: m.AsOf, Reason: "unpublished"}
	}
	series := m.Series
	newest := 0
	for _, r := range ds.Rows {
		y, mo := pick(r).Series.Last()
		newest = max(newest, y*12+mo-1)
	}
	y, mo := series.Last()
	return Chart{Series: &series, Source: m.Source, AsOf: m.AsOf, Behind: newest - (y*12 + mo - 1)}
}

func currencyChart(c reference.Country, ds history.Dataset) Chart {
	if c.Currency == nil {
		return Chart{Source: ecb.Source, Reason: "euro"}
	}
	cur := ds.Currencies[*c.Currency]
	if cur.Series == nil {
		return Chart{Source: cur.Source, AsOf: cur.AsOf, Derived: cur.Derived, Reason: firstNonEmpty(cur.Reason, "not published by the ECB")}
	}
	series := *cur.Series
	newest := 0
	for _, o := range ds.Currencies {
		if o.Series != nil {
			y, mo := o.Series.Last()
			newest = max(newest, y*12+mo-1)
		}
	}
	y, mo := series.Last()
	return Chart{Series: &series, Source: cur.Source, AsOf: cur.AsOf, Derived: cur.Derived, Behind: newest - (y*12 + mo - 1)}
}

// composition turns the World Bank's four sector shares into the bar, adding
// the residual (taxes less subsidies) as 100 less the three sectors.
func composition(c worldbank.Composition) CityComposition {
	out := CityComposition{
		Agriculture: sourced(c.Agriculture), Industry: sourced(c.Industry),
		Services: sourced(c.Services), Manufacturing: sourced(c.Manufacturing),
	}
	res := Sourced{Unit: "% of GDP", Source: "derived: 100 less agriculture, industry and services", AsOf: c.Industry.AsOf}
	if c.Agriculture.Value != nil && c.Industry.Value != nil && c.Services.Value != nil {
		v := math.Round((100-*c.Agriculture.Value-*c.Industry.Value-*c.Services.Value)*10) / 10
		res.Value = &v
		res.AsOf = oldest(c.Agriculture.AsOf, c.Industry.AsOf, c.Services.AsOf)
	} else {
		res.Reason = "a sector share is unpublished"
	}
	out.Residual = res
	return out
}

func oldest(years ...string) string {
	o := years[0]
	for _, y := range years {
		if strings.Compare(y, o) < 0 {
			o = y
		}
	}
	return o
}
