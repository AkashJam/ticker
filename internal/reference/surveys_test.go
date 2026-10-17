package reference

import (
	"context"
	"math"
	"slices"
	"testing"
)

// The city records are hand-maintained, so the file is checked as data: every
// record belongs to a country with a city, every total is the sum of its parts,
// and nothing unverified can be shown (research/city-surveys.md).
func TestSurveyRecords(t *testing.T) {
	st := New(nil, Embedded(), nil)
	ctx := context.Background()
	rows, err := st.Surveys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 21 {
		t.Fatalf("%d survey records, want 21", len(rows))
	}
	countries, err := st.AtlasCountries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bySlug := map[string]Country{}
	for _, c := range countries {
		bySlug[c.Slug] = c
	}

	seen := map[string]bool{}
	for _, s := range rows {
		c, ok := bySlug[s.Slug]
		if s.Slug == "singapore" {
			// The one city with a survey and no Atlas row (bearing.md: it appears only
			// as a city page). It has no page until the country table and the fetchers
			// cover it, so it must stay unverified.
			if s.Verified {
				t.Error("singapore: verified, but it has no country row to serve a page from")
			}
			if s.Headline.Currency != "SGD" {
				t.Errorf("singapore: headline in %s", s.Headline.Currency)
			}
			seen[s.Slug] = true
			continue
		}
		if !ok {
			t.Errorf("%s: no Atlas row with this slug", s.Slug)
			continue
		}
		if seen[s.Slug] {
			t.Errorf("%s: two records", s.Slug)
		}
		seen[s.Slug] = true

		if s.Headline.Value <= 0 || s.Headline.Year < 2000 || s.Headline.Label == "" || s.Headline.Method == "" {
			t.Errorf("%s: incomplete headline %+v", s.Slug, s.Headline)
		}
		want := "EUR"
		if c.Currency != nil {
			want = *c.Currency
		}
		if s.Headline.Currency != want {
			t.Errorf("%s: headline in %s, the country's currency is %s", s.Slug, s.Headline.Currency, want)
		}
		if !slices.Contains(HousingTags, s.Housing.Tag) || s.Housing.Note == "" {
			t.Errorf("%s: housing tag %q or empty note", s.Slug, s.Housing.Tag)
		}
		if len(s.Sources) == 0 {
			t.Errorf("%s: no source", s.Slug)
		}
		for _, src := range s.Sources {
			if src.Label == "" || src.Retrieved == "" {
				t.Errorf("%s: a source without a label or a retrieval date: %+v", s.Slug, src)
			}
		}
		if s.Measure != "" && s.Measure != MeasureAverage {
			t.Errorf("%s: measure %q", s.Slug, s.Measure)
		}
		if (s.Measure == MeasureAverage) != (s.Slug == "china" || s.Slug == "india") {
			t.Errorf("%s: measure %q; only Shanghai's and Mumbai's surveys report an average", s.Slug, s.Measure)
		}
		if s.Instrument == "" || s.Publisher == "" {
			t.Errorf("%s: no instrument or publisher", s.Slug)
		}
		// Either a split or the reason there is none, never both and never neither.
		if (len(s.Parts) == 0) == (s.SplitNote == nil) {
			t.Errorf("%s: %d parts and split note %v; want exactly one of the two", s.Slug, len(s.Parts), s.SplitNote)
		}
		if len(s.Parts) > 0 {
			var amount, share float64
			for _, p := range s.Parts {
				if p.Label == "" || p.Amount <= 0 {
					t.Errorf("%s: bad part %+v", s.Slug, p)
				}
				amount += p.Amount
				share += p.Share
			}
			// Surveys round their own parts, so the sum is the total within a rounding.
			if math.Abs(amount/s.Headline.Value-1) > 0.005 {
				t.Errorf("%s: parts sum to %.2f against a headline of %.2f", s.Slug, amount, s.Headline.Value)
			}
			if math.Abs(share-100) > 0.5 {
				t.Errorf("%s: shares sum to %.1f", s.Slug, share)
			}
		}
		for _, l := range s.SecondLines {
			if l.Label == "" || l.Value <= 0 {
				t.Errorf("%s: bad second line %+v", s.Slug, l)
			}
		}
		if !s.Verified && len(s.OpenItems) == 0 {
			t.Errorf("%s: unverified with no open item saying why", s.Slug)
		}
		// A city page ships only for a verified record; step 4 flips the flag.
		if c.HasCityPage && !s.Verified {
			t.Errorf("%s: hasCityPage is true for an unverified record", s.Slug)
		}
	}
}

func TestOnlyTheOpenItemsAreUnverified(t *testing.T) {
	st := New(nil, Embedded(), nil)
	rows, err := st.Surveys(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var unverified []string
	for _, s := range rows {
		if !s.Verified {
			unverified = append(unverified, s.Slug)
		}
	}
	slices.Sort(unverified)
	want := []string{"korea", "norway", "singapore", "south-africa"}
	if !slices.Equal(unverified, want) {
		t.Errorf("unverified %v, want %v: close an item by setting its flag and editing this list", unverified, want)
	}
}

// hasCityPage is derived from the verified record, so it cannot outrun the data.
func TestCityPagesFollowTheVerifiedRecords(t *testing.T) {
	st := New(nil, Embedded(), nil)
	ctx := context.Background()
	rows, err := st.Surveys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	verified := map[string]bool{}
	for _, s := range rows {
		verified[s.Slug] = s.Verified
	}
	all, err := st.Countries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pages := 0
	for _, c := range all {
		if c.HasCityPage != (c.Atlas && verified[c.Slug]) {
			t.Errorf("%s: hasCityPage %v, verified %v", c.Slug, c.HasCityPage, verified[c.Slug])
		}
		if c.HasCityPage {
			pages++
		}
	}
	if pages != 17 {
		t.Errorf("%d city pages, want 17 (the verified records with an Atlas row)", pages)
	}
}

func TestSurveyLookup(t *testing.T) {
	st := New(nil, Embedded(), nil)
	s, ok, err := st.Survey(context.Background(), "japan")
	if err != nil || !ok || s.Headline.Value != 130920 {
		t.Fatalf("japan = %+v ok=%v err=%v", s, ok, err)
	}
	if _, ok, _ := st.Survey(context.Background(), "colombia"); ok {
		t.Error("colombia has no survey and no Atlas row")
	}
}
