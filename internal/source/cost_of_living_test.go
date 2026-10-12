package source

import (
	"context"
	"math"
	"testing"

	"github.com/AkashJam/ticker/internal/change"
)

// annualCPI builds an annual index from 2010 with the given values.
func annualCPI(v ...float64) change.Series {
	return change.Series{Start: "2010-01", Step: 12, Values: v}
}

func cpiFor(m map[string]change.Series) CPIFunc {
	return func(_ context.Context, code string) (change.Series, bool) {
		s, ok := m[code]
		return s, ok
	}
}

func near(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 0.005 {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

func TestCarriedToTheLatestCPIYearAndNoFurther(t *testing.T) {
	// Milan: ISTAT 972 in 2024. CPI 100 in 2010 rising to 120 in 2024, 126 in 2025.
	idx := make([]float64, 16)
	for i := range idx {
		idx[i] = 100
	}
	idx[14], idx[15] = 120, 126 // 2024, 2025
	d, ok := CostOfLivingByCity(context.Background(), cpiFor(map[string]change.Series{"IT": annualCPI(idx...)}), "milan")
	if !ok {
		t.Fatal("Milan not found")
	}
	// 972 × 126 ÷ 120, carried to 2025 (the latest CPI year) and not to today.
	near(t, "local value", d.LocalValue, 1020.6)
	near(t, "changeYoY", d.ChangeYoY, 5) // 126 against 120
	if d.Simulated != true || d.SurveyYear != 2024 {
		t.Errorf("detail = %+v", d.CostOfLivingSnapshot)
	}
	if len(d.Series) != 2 || d.Series[0].Time.Year() != 2024 || d.Series[1].Time.Year() != 2025 {
		t.Fatalf("series = %+v, want 2024 and 2025", d.Series)
	}
	near(t, "series start", d.Series[0].Value, 972)
	near(t, "series end", d.Series[1].Value, 1020.6)
	near(t, "basket", d.Basket[0].Value, 1020.6*0.45)
}

func TestNoCPIServesTheBaselineAsPublished(t *testing.T) {
	for name, cpi := range map[string]CPIFunc{"nil": nil, "no country": cpiFor(nil)} {
		snaps := CostOfLiving(context.Background(), cpi)
		if len(snaps) != len(colSeed) {
			t.Fatalf("%s: %d snapshots", name, len(snaps))
		}
		for i, s := range snaps {
			near(t, name+" "+s.City, s.LocalValue, colSeed[i].LocalValue)
			if s.ChangeYoY != 0 || !s.Simulated {
				t.Errorf("%s %s: %+v", name, s.City, s)
			}
		}
	}
	d, _ := CostOfLivingByCity(context.Background(), nil, "Tokyo")
	if len(d.Series) != 1 || d.Series[0].Time.Year() != 2024 {
		t.Errorf("series = %+v, want the survey year alone", d.Series)
	}
}

func TestACPIThatStopsBeforeTheSurveyIsNotExtrapolated(t *testing.T) {
	// New York is a 2025 survey; the US has no 2025 CPI yet, so its CPI ends in
	// 2024. Carrying the baseline backwards would invent a figure.
	us := annualCPI(100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114) // 2010..2024
	s := CostOfLiving(context.Background(), cpiFor(map[string]change.Series{"US": us}))
	for _, snap := range s {
		if snap.City == "New York" {
			near(t, "New York", snap.LocalValue, 4833)
			if snap.ChangeYoY != 0 {
				t.Errorf("changeYoY = %v, want 0", snap.ChangeYoY)
			}
		}
	}
}

func TestUnknownCityAndCaseInsensitiveLookup(t *testing.T) {
	if _, ok := CostOfLivingByCity(context.Background(), nil, "Atlantis"); ok {
		t.Error("unknown city found")
	}
	if _, ok := CostOfLivingByCity(context.Background(), nil, "LONDON"); !ok {
		t.Error("lookup is case-sensitive")
	}
}

func TestEverySeedHasACodeAndNoDrift(t *testing.T) {
	for _, c := range colSeed {
		if len(c.Code) != 2 {
			t.Errorf("%s: code %q", c.City, c.Code)
		}
	}
	// The illustrative drift is gone: with the same CPI at both ends a baseline
	// does not move.
	flat := annualCPI(100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100)
	m := map[string]change.Series{"IT": flat, "FR": flat, "GB": flat, "JP": flat, "US": flat}
	for i, s := range CostOfLiving(context.Background(), cpiFor(m)) {
		near(t, s.City, s.LocalValue, colSeed[i].LocalValue)
	}
}
