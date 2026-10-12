package source

import (
	"context"
	"strings"
	"time"

	"github.com/AkashJam/ticker/internal/change"
)

// The Cost-of-Living family (portfolio.md §6.2) is deliberately not a
// random walk like the SIM: symbols above — it's seeded from real official
// surveys and carried forward by each country's real CPI (World Bank
// FP.CPI.TOTL, Phase 10), so "simulated" here means "extrapolated from a
// real baseline," not "fabricated." Phase 14 deletes this whole family. It
// never flows through the tick/candle pipeline (§6.3: "col is served from
// the sim family, not a table") — CostOfLiving/CostOfLivingDetail below are
// computed fresh on every read.

// colCity is the static per-city seed: the survey baseline, carried forward on
// the country's CPI. Values and citations are exactly portfolio.md §6.2's
// table; FX rates are its dated (4 Aug 2026) EUR-Lex mid-market rates. The FX
// rate is not a live feed, hence always `simulated: true` downstream.
type colCity struct {
	City          string
	Country       string
	Code          string // ISO 3166 alpha-2, the key into the reference datasets
	LocalCurrency string
	LocalValue    float64 // survey-year baseline, in LocalCurrency
	SurveyYear    int
	Source        string
	Method        string
	FXToEUR       float64 // 1 unit of LocalCurrency in EUR
}

var colSeed = []colCity{
	{
		City: "Milan", Country: "Italy", Code: "IT", LocalCurrency: "EUR", LocalValue: 972,
		SurveyYear: 2024, Source: "ISTAT absolute-poverty basket",
		Method: "Absolute poverty threshold", FXToEUR: 1.0,
	},
	{
		City: "Paris", Country: "France", Code: "FR", LocalCurrency: "EUR", LocalValue: 1634,
		SurveyYear: 2022, Source: "ONPES budget de référence",
		Method: "Reference budget (budget de référence)", FXToEUR: 1.0,
	},
	{
		City: "London", Country: "United Kingdom", Code: "GB", LocalCurrency: "GBP", LocalValue: 2333,
		SurveyYear: 2024, Source: "JRF / Loughborough — MIS",
		Method: "Minimum Income Standard (MIS)", FXToEUR: 1.1677,
	},
	{
		City: "Tokyo", Country: "Japan", Code: "JP", LocalCurrency: "JPY", LocalValue: 130700,
		SurveyYear: 2024, Source: "MHLW Seikatsu Hogo welfare std",
		Method: "Livelihood protection standard (Seikatsu Hogo)", FXToEUR: 0.005517,
	},
	{
		City: "New York", Country: "United States", Code: "US", LocalCurrency: "USD", LocalValue: 4833,
		SurveyYear: 2025, Source: "MIT living wage (pre-tax)",
		Method: "Living wage calculator, pre-tax", FXToEUR: 0.8684,
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

// CPIFunc returns a country's annual CPI index (an annual change.Series from
// 2010) for the given ISO code, or false when there is none. The server backs
// it with the reference store, so it works with Redis down.
type CPIFunc func(ctx context.Context, code string) (change.Series, bool)

// carried is a baseline carried forward on a country's CPI.
type carried struct {
	index change.Series
	ok    bool
}

func (k carried) at(year int) (float64, bool) {
	if !k.ok {
		return 0, false
	}
	return k.index.At(year, 1)
}

// latest is the last year the CPI covers.
func (k carried) latest() int {
	y, _ := k.index.Last()
	return y
}

// value is the survey baseline carried to year on the CPI: baseline × CPI(year)
// ÷ CPI(survey year). It is carried to the latest published CPI year and no
// further, and never backwards past the survey (bearing.md #98); with no usable
// CPI it is the baseline as published.
func (k carried) value(c colCity, year int) float64 {
	base, okBase := k.at(c.SurveyYear)
	cur, okCur := k.at(year)
	if !okBase || !okCur || year < c.SurveyYear || base <= 0 {
		return c.LocalValue
	}
	return c.LocalValue * cur / base
}

// lastYear is the year to carry to: the latest CPI year, if the CPI reaches the
// survey; otherwise the survey year itself.
func (k carried) lastYear(c colCity) int {
	if _, ok := k.at(c.SurveyYear); !ok || k.latest() < c.SurveyYear {
		return c.SurveyYear
	}
	return k.latest()
}

func carriedFor(ctx context.Context, cpi CPIFunc, c colCity) carried {
	if cpi == nil {
		return carried{}
	}
	s, ok := cpi(ctx, c.Code)
	return carried{index: s, ok: ok}
}

// CostOfLiving returns every tracked city's snapshot, carried on real CPI.
func CostOfLiving(ctx context.Context, cpi CPIFunc) []CostOfLivingSnapshot {
	out := make([]CostOfLivingSnapshot, 0, len(colSeed))
	for _, c := range colSeed {
		out = append(out, snapshotFor(c, carriedFor(ctx, cpi, c)))
	}
	return out
}

// CostOfLivingByCity returns one city's detail, including its series and
// expense basket. ok is false if city doesn't match a tracked city.
func CostOfLivingByCity(ctx context.Context, cpi CPIFunc, city string) (CostOfLivingDetail, bool) {
	for _, c := range colSeed {
		if !strings.EqualFold(c.City, city) {
			continue
		}
		k := carriedFor(ctx, cpi, c)
		snap := snapshotFor(c, k)
		return CostOfLivingDetail{
			CostOfLivingSnapshot: snap,
			Method:               c.Method,
			FXToEUR:              c.FXToEUR,
			Series:               seriesFor(c, k),
			Basket:               basketFor(snap.LocalValue),
		}, true
	}
	return CostOfLivingDetail{}, false
}

func snapshotFor(c colCity, k carried) CostOfLivingSnapshot {
	year := k.lastYear(c)
	local := k.value(c, year)
	changeYoY := 0.0
	if prev, ok := k.at(year - 1); ok && year-1 >= c.SurveyYear && prev > 0 {
		cur, _ := k.at(year)
		changeYoY = (cur/prev - 1) * 100
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

// seriesFor is one point a year, from the survey year to the latest CPI year,
// so the city detail page can render a trend rather than a single value.
func seriesFor(c colCity, k carried) []CostOfLivingSeriesPoint {
	// Non-nil for the same reason as the store layer's Candles/Symbols: a nil
	// slice would marshal to JSON `null` against the documented array contract.
	points := make([]CostOfLivingSeriesPoint, 0)
	for y := c.SurveyYear; y <= k.lastYear(c); y++ {
		points = append(points, CostOfLivingSeriesPoint{
			Time:  time.Date(y, time.January, 1, 0, 0, 0, 0, time.UTC),
			Value: roundToCents(k.value(c, y)),
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
