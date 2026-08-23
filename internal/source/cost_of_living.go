package source

import (
	"math"
	"strings"
	"time"
)

// The Cost-of-Living family (portfolio.md §6.2) is deliberately not a
// random walk like the SIM: symbols above — it's seeded from real official
// surveys and drifted forward by each country's inflation, so "simulated"
// here means "extrapolated from a real baseline," not "fabricated." It
// never flows through the tick/candle pipeline (§6.3: "col is served from
// the sim family, not a table") — CostOfLiving/CostOfLivingDetail below are
// computed fresh on every read.

// colCity is the static per-city seed: the survey baseline plus an
// illustrative annual inflation rate used to drift it forward. Values and
// citations are exactly portfolio.md §6.2's table; FX rates are its dated
// (4 Aug 2026) EUR-Lex mid-market rates. Inflation rates are simplified,
// illustrative annual figures for this simulated demo dataset — not a live
// economic feed — hence always `simulated: true` downstream.
type colCity struct {
	City          string
	Country       string
	LocalCurrency string
	LocalValue    float64 // survey-year baseline, in LocalCurrency
	SurveyYear    int
	Source        string
	Method        string
	FXToEUR       float64 // 1 unit of LocalCurrency in EUR
	AnnualInflate float64 // illustrative annual drift rate, e.g. 0.02 = 2%/yr
}

var colSeed = []colCity{
	{
		City: "Milan", Country: "Italy", LocalCurrency: "EUR", LocalValue: 972,
		SurveyYear: 2024, Source: "ISTAT absolute-poverty basket",
		Method: "Absolute poverty threshold", FXToEUR: 1.0, AnnualInflate: 0.020,
	},
	{
		City: "Paris", Country: "France", LocalCurrency: "EUR", LocalValue: 1634,
		SurveyYear: 2022, Source: "ONPES budget de référence",
		Method: "Reference budget (budget de référence)", FXToEUR: 1.0, AnnualInflate: 0.025,
	},
	{
		City: "London", Country: "United Kingdom", LocalCurrency: "GBP", LocalValue: 2333,
		SurveyYear: 2024, Source: "JRF / Loughborough — MIS",
		Method: "Minimum Income Standard (MIS)", FXToEUR: 1.1677, AnnualInflate: 0.030,
	},
	{
		City: "Tokyo", Country: "Japan", LocalCurrency: "JPY", LocalValue: 130700,
		SurveyYear: 2024, Source: "MHLW Seikatsu Hogo welfare std",
		Method: "Livelihood protection standard (Seikatsu Hogo)", FXToEUR: 0.005517, AnnualInflate: 0.020,
	},
	{
		City: "New York", Country: "United States", LocalCurrency: "USD", LocalValue: 4833,
		SurveyYear: 2025, Source: "MIT living wage (pre-tax)",
		Method: "Living wage calculator, pre-tax", FXToEUR: 0.8684, AnnualInflate: 0.030,
	},
}

// basketShares is a simplified, illustrative proportional expense
// breakdown applied to every city's current drifted value — real per-city,
// per-category source figures aren't available from these surveys, and
// fabricating false per-category precision would be worse than an honestly
// approximate breakdown labeled as such in the API's `simulated` flag.
var basketShares = []struct {
	Label string
	Share float64
}{
	{"Housing", 0.45},
	{"Food", 0.20},
	{"Transport", 0.12},
	{"Utilities", 0.10},
	{"Other", 0.13},
}

// CostOfLivingSnapshot is one city's current (drifted-to-today) figures —
// the shape GET /col returns. JSON tags match §8's example exactly.
type CostOfLivingSnapshot struct {
	City          string  `json:"city"`
	Country       string  `json:"country"`
	LocalCurrency string  `json:"localCurrency"`
	LocalValue    float64 `json:"localValue"`
	EurValue      float64 `json:"eurValue"`
	ChangeYoY     float64 `json:"changeYoY"`
	SurveyYear    int     `json:"surveyYear"`
	Source        string  `json:"source"`
	Simulated     bool    `json:"simulated"`
}

// CostOfLivingSeriesPoint is one point in a city's drifted-value history.
type CostOfLivingSeriesPoint struct {
	Time  time.Time `json:"time"`
	Value float64   `json:"value"`
}

// CostOfLivingBasketItem is one category in a city's expense breakdown.
type CostOfLivingBasketItem struct {
	Label string  `json:"label"`
	Value float64 `json:"value"`
}

// CostOfLivingDetail is the full shape GET /col/{city} returns.
type CostOfLivingDetail struct {
	CostOfLivingSnapshot
	Method  string                    `json:"method"`
	FXToEUR float64                   `json:"fxToEur"`
	Series  []CostOfLivingSeriesPoint `json:"series"`
	Basket  []CostOfLivingBasketItem  `json:"basket"`
}

// driftedValue compounds a survey-year baseline forward to `at` at the
// city's illustrative annual inflation rate.
func driftedValue(c colCity, at time.Time) float64 {
	surveyStart := time.Date(c.SurveyYear, time.January, 1, 0, 0, 0, 0, time.UTC)
	years := at.Sub(surveyStart).Hours() / (24 * 365.25)
	if years < 0 {
		years = 0
	}
	return c.LocalValue * math.Pow(1+c.AnnualInflate, years)
}

// CostOfLiving returns every tracked city's current drifted snapshot.
func CostOfLiving() []CostOfLivingSnapshot {
	now := time.Now().UTC()
	out := make([]CostOfLivingSnapshot, 0, len(colSeed))
	for _, c := range colSeed {
		out = append(out, snapshotFor(c, now))
	}
	return out
}

// CostOfLivingByCity returns one city's detail, including its drift series
// and expense basket. ok is false if city doesn't match a tracked city.
func CostOfLivingByCity(city string) (CostOfLivingDetail, bool) {
	now := time.Now().UTC()
	for _, c := range colSeed {
		if !strings.EqualFold(c.City, city) {
			continue
		}
		snap := snapshotFor(c, now)
		return CostOfLivingDetail{
			CostOfLivingSnapshot: snap,
			Method:               c.Method,
			FXToEUR:              c.FXToEUR,
			Series:               seriesFor(c, now),
			Basket:               basketFor(snap.LocalValue),
		}, true
	}
	return CostOfLivingDetail{}, false
}

func snapshotFor(c colCity, now time.Time) CostOfLivingSnapshot {
	local := driftedValue(c, now)
	localOneYearAgo := driftedValue(c, now.AddDate(-1, 0, 0))
	changeYoY := 0.0
	if localOneYearAgo > 0 {
		changeYoY = (local - localOneYearAgo) / localOneYearAgo * 100
	}
	return CostOfLivingSnapshot{
		City:          c.City,
		Country:       c.Country,
		LocalCurrency: c.LocalCurrency,
		LocalValue:    roundToCents(local),
		EurValue:      roundToCents(local * c.FXToEUR),
		ChangeYoY:     roundToCents(changeYoY),
		SurveyYear:    c.SurveyYear,
		Source:        c.Source,
		Simulated:     true,
	}
}

// seriesFor generates one monthly point per month from the survey baseline
// to now, so the city detail page can render a real (if illustrative)
// trend line rather than a single current value.
func seriesFor(c colCity, now time.Time) []CostOfLivingSeriesPoint {
	surveyStart := time.Date(c.SurveyYear, time.January, 1, 0, 0, 0, 0, time.UTC)
	// Non-nil for the same reason as the store layer's Candles/Symbols —
	// the loop bound makes an empty result practically unreachable today
	// (surveyStart is always in the past), but a nil slice here would
	// still marshal to JSON `null` against the documented array contract
	// if that ever stopped being true.
	points := make([]CostOfLivingSeriesPoint, 0)
	for t := surveyStart; !t.After(now); t = t.AddDate(0, 1, 0) {
		points = append(points, CostOfLivingSeriesPoint{
			Time:  t,
			Value: roundToCents(driftedValue(c, t)),
		})
	}
	return points
}

func basketFor(currentLocalValue float64) []CostOfLivingBasketItem {
	items := make([]CostOfLivingBasketItem, 0, len(basketShares))
	for _, b := range basketShares {
		items = append(items, CostOfLivingBasketItem{
			Label: b.Label,
			Value: roundToCents(currentLocalValue * b.Share),
		})
	}
	return items
}
