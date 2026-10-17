package atlas

import (
	"context"
	"errors"
	"math"
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
