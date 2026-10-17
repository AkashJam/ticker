package atlas

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/AkashJam/ticker/internal/ecb"
	"github.com/AkashJam/ticker/internal/history"
	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/worldbank"
)

// embeddedInput reads the committed snapshots, as the ticker does with Redis down.
func embeddedInput(t *testing.T) CityInput {
	t.Helper()
	st := reference.New(nil, reference.Embedded(), nil)
	ctx := context.Background()
	countries, err := st.AtlasCountries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	surveys, err := st.Surveys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	in := CityInput{Countries: countries, Surveys: surveys}
	for name, v := range map[string]any{history.DatasetName: &in.History, ecb.DatasetName: &in.ECB, worldbank.DatasetName: &in.WB} {
		if _, err := st.Get(ctx, name, v); err != nil {
			t.Fatal(err)
		}
	}
	return in
}

func near(t *testing.T, name string, got *float64, want, tol float64) {
	t.Helper()
	if got == nil || math.Abs(*got-want) > tol {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

func TestCityIsCarriedOnCPIToTheLatestYearAndConvertedAtADatedRate(t *testing.T) {
	in := embeddedInput(t)
	// Dijon: ONPES priced in 2014, euro area. Carried on France's CPI to the latest
	// published year; the euro needs no rate.
	r, err := BuildCity("france", in)
	if err != nil {
		t.Fatal(err)
	}
	if r.City != "Dijon" || r.Country != "France" || !r.Verified {
		t.Errorf("header %+v", r)
	}
	c := r.Cost.Carried
	if c.Value == nil || c.From == nil || c.To == nil || c.Factor == nil || c.From.Year != 2014 || c.To.Year <= 2014 || c.Year != c.To.Year {
		t.Fatalf("carried %+v", c)
	}
	near(t, "carried", c.Value, 1571**c.Factor, 0.01)
	near(t, "factor", c.Factor, c.To.Index/c.From.Index, 0.0001)
	if *c.Value <= 1571 {
		t.Errorf("twelve years of inflation left the figure at %v", *c.Value)
	}
	if r.Cost.Euro.Reason != "euro" || r.Cost.Euro.Value == nil || *r.Cost.Euro.Value != *c.Value || r.Cost.Euro.Basis != "carried" {
		t.Errorf("euro %+v", r.Cost.Euro)
	}
}

func TestCityNewerThanTheLatestCPIIsNotCarried(t *testing.T) {
	in := embeddedInput(t)
	// Tokyo: an October 2026 standard against Japan's latest CPI year (2025).
	r, err := BuildCity("japan", in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Cost.Carried.Value != nil || r.Cost.Carried.Reason == "" || r.Cost.Carried.To == nil {
		t.Fatalf("carried %+v", r.Cost.Carried)
	}
	e := r.Cost.Euro
	if e.Basis != "survey" || e.Rate == nil || e.Value == nil || e.Year != 2025 {
		t.Fatalf("euro %+v: a 2026 figure converts at the latest complete year's average, 2025", e)
	}
	near(t, "euro", e.Value, 130920 / *e.Rate, 0.01)
	if len(r.Survey.SecondLines) != 1 || r.Survey.SecondLines[0].Eur == nil {
		t.Fatalf("second lines %+v", r.Survey.SecondLines)
	}
	near(t, "excluding housing in euros", r.Survey.SecondLines[0].Eur, 77220 / *e.Rate, 0.01)
	if len(r.Survey.Parts) != 4 || !r.Survey.Parts[0].Hatched {
		t.Errorf("parts %+v", r.Survey.Parts)
	}
}

func TestCityChartsRunFromTheFirstObservation(t *testing.T) {
	in := embeddedInput(t)
	r, err := BuildCity("japan", in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Charts.Shares.Series == nil || r.Charts.Shares.Series.Start >= "1970" {
		t.Errorf("shares %+v", r.Charts.Shares.Series)
	}
	if r.Charts.Currency.Series == nil || r.Charts.Currency.Series.Start != "1999-01" {
		t.Errorf("currency %+v", r.Charts.Currency.Series)
	}
	// Japan's house prices end a quarter behind the newest country's (the mock's note).
	h := r.Charts.Housing
	if h.Series == nil || h.Behind != 3 || h.AsOf != "2025-Q4" {
		t.Errorf("housing behind %d as of %s", h.Behind, h.AsOf)
	}
	// The euro area has no rate against itself, and says so rather than drawing a flat line.
	it, err := BuildCity("italy", in)
	if err != nil {
		t.Fatal(err)
	}
	if it.Charts.Currency.Series != nil || it.Charts.Currency.Reason != "euro" {
		t.Errorf("italy currency %+v", it.Charts.Currency)
	}
	if it.Survey.Parts != nil || it.Survey.SplitNote == nil {
		t.Errorf("italy publishes a total, not a split: %+v", it.Survey)
	}
}

func TestCityMacroAndComposition(t *testing.T) {
	in := embeddedInput(t)
	r, err := BuildCity("japan", in)
	if err != nil {
		t.Fatal(err)
	}
	m := r.Macro
	for name, s := range map[string]Sourced{"spending": m.EverydaySpendingEur, "gni": m.GniPpp, "unemployment": m.Unemployment, "life": m.LifeExpectancy, "inflation": m.Inflation} {
		if s.Value == nil || s.Source == "" || s.AsOf == "" {
			t.Errorf("%s = %+v", name, s)
		}
	}
	comp := r.Composition
	near(t, "residual", comp.Residual.Value, 100-*comp.Agriculture.Value-*comp.Industry.Value-*comp.Services.Value, 0.06)
	if comp.Residual.Source == "" || comp.Residual.AsOf == "" {
		t.Errorf("residual %+v", comp.Residual)
	}
}

func TestEveryAtlasCityWithASurveyBuilds(t *testing.T) {
	in := embeddedInput(t)
	built := 0
	for _, s := range in.Surveys {
		r, err := BuildCity(s.Slug, in)
		if s.Slug == "singapore" {
			if !errors.Is(err, ErrNoCity) {
				t.Errorf("singapore: %v, want ErrNoCity (no country row)", err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", s.Slug, err)
			continue
		}
		built++
		if r.Cost.Euro.Value == nil && r.Cost.Euro.Reason == "" {
			t.Errorf("%s: no euro value and no reason", s.Slug)
		}
		if r.Charts.Shares.Series == nil || r.Charts.Housing.Series == nil {
			t.Errorf("%s: a long chart is missing", s.Slug)
		}
	}
	if built != 20 {
		t.Errorf("%d cities built, want 20", built)
	}
	if _, err := BuildCity("chile", in); !errors.Is(err, ErrNoCity) {
		t.Errorf("chile: %v", err)
	}
}

func TestAffordabilityNamesBothLegsAndTheirGap(t *testing.T) {
	in := embeddedInput(t)
	// Tokyo: an October 2026 standard against 2024 spending, two years apart, in a
	// country whose prices rose 3.2%: stated, not flagged.
	tk, err := BuildCity("japan", in)
	if err != nil {
		t.Fatal(err)
	}
	a := tk.Affordability
	if a.Ratio == nil || a.Cost.Eur == nil || a.Spending.Value == nil || a.Measure != "floor" {
		t.Fatalf("tokyo %+v", a)
	}
	near(t, "ratio", a.Ratio, *a.Cost.Eur / *a.Spending.Value * 100, 0.06)
	if a.GapYears != 2 || a.Direction != "cost-newer" || a.Cost.PriceYear != 2026 || a.Cost.Basis != "survey" || a.Spending.AsOf != "2024" || a.Caveat != "" {
		t.Errorf("tokyo legs %+v", a)
	}
	// Dijon: carried to the latest CPI year, so the legs share a year.
	dj, _ := BuildCity("france", in)
	if g := dj.Affordability; g.GapYears != 0 || g.Direction != "same" || g.Cost.Basis != "carried" || g.Cost.PriceYear != dj.Cost.Carried.Year || g.Ratio == nil {
		t.Errorf("dijon %+v", g)
	}
}

func TestAffordabilityFlagsAFastMovingPriceLevel(t *testing.T) {
	in := embeddedInput(t)
	// Türkiye: a September 2026 lira figure against 2025 spending, with prices
	// rising fast: the ratio is overstated, and the page says so with the figure.
	tr, err := BuildCity("turkiye", in)
	if err != nil {
		t.Fatal(err)
	}
	a := tr.Affordability
	if a.Ratio == nil || a.Direction != "cost-newer" || a.GapYears < 1 || a.Inflation.Value == nil || *a.Inflation.Value <= InflationFlag {
		t.Fatalf("turkiye %+v", a)
	}
	if !strings.Contains(a.Caveat, "Türkiye's prices rose") || !strings.Contains(a.Caveat, "overstated") {
		t.Errorf("caveat %q", a.Caveat)
	}
}

func TestAffordabilityWordsAverageSurveysApart(t *testing.T) {
	in := embeddedInput(t)
	for slug, want := range map[string]string{"china": "average", "india": "average", "japan": "floor", "italy": "floor"} {
		r, err := BuildCity(slug, in)
		if err != nil {
			t.Fatal(err)
		}
		if r.Affordability.Measure != want {
			t.Errorf("%s measure %q, want %q", slug, r.Affordability.Measure, want)
		}
	}
}

func TestEveryVerifiedCityHasARatioAndNoRatioIsInventedForAMissingLeg(t *testing.T) {
	in := embeddedInput(t)
	for _, s := range in.Surveys {
		if !s.Verified || s.Slug == "singapore" {
			continue
		}
		r, err := BuildCity(s.Slug, in)
		if err != nil {
			t.Fatal(err)
		}
		if r.Affordability.Ratio == nil || r.Affordability.Reason != "" {
			t.Errorf("%s: %+v", s.Slug, r.Affordability)
		}
	}
	// A leg that is missing gives a reason and no ratio.
	broken := embeddedInput(t)
	row := broken.WB.Rows["JP"]
	row.EverydaySpendingEur.Value = nil
	broken.WB.Rows["JP"] = row
	r, _ := BuildCity("japan", broken)
	if r.Affordability.Ratio != nil || r.Affordability.Reason == "" {
		t.Errorf("missing spending: %+v", r.Affordability)
	}
}
