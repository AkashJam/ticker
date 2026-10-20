package reference

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/store"
)

type fakeCache struct {
	data   map[string]string
	getErr error
	setErr error
	ttl    time.Duration
}

func newFakeCache() *fakeCache { return &fakeCache{data: map[string]string{}} }

func (f *fakeCache) CacheGet(_ context.Context, key string) (string, error) {
	if f.getErr != nil {
		return "", f.getErr
	}
	v, ok := f.data[key]
	if !ok {
		return "", store.ErrCacheMiss
	}
	return v, nil
}

func (f *fakeCache) CacheSet(_ context.Context, key, value string, ttl time.Duration) error {
	if f.setErr != nil {
		return f.setErr
	}
	f.data[key] = value
	f.ttl = ttl
	return nil
}

type blob struct {
	Source string `json:"source"`
	AsOf   string `json:"asOf"`
	N      int    `json:"n"`
}

func testStore(c Cache) *Store {
	fsys := fstest.MapFS{"demo.json": {Data: []byte(`{"source":"embedded","asOf":"2026","n":1}`)}}
	return New(c, fsys, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestGetCacheDownServesEmbedded(t *testing.T) {
	c := newFakeCache()
	c.getErr = errors.New("dial tcp: connection refused")
	var got blob
	src, err := testStore(c).Get(context.Background(), "demo", &got)
	if err != nil || src != SourceEmbedded || got.N != 1 {
		t.Fatalf("got src=%q err=%v blob=%+v, want embedded n=1", src, err, got)
	}
}

func TestGetNilCacheServesEmbedded(t *testing.T) {
	var got blob
	src, err := testStore(nil).Get(context.Background(), "demo", &got)
	if err != nil || src != SourceEmbedded {
		t.Fatalf("got src=%q err=%v, want embedded", src, err)
	}
}

func TestGetMissServesEmbedded(t *testing.T) {
	var got blob
	src, err := testStore(newFakeCache()).Get(context.Background(), "demo", &got)
	if err != nil || src != SourceEmbedded || got.N != 1 {
		t.Fatalf("got src=%q err=%v blob=%+v", src, err, got)
	}
}

func TestGetCorruptPayloadServesEmbedded(t *testing.T) {
	c := newFakeCache()
	c.data[CacheKey("demo")] = `{not json`
	var got blob
	src, err := testStore(c).Get(context.Background(), "demo", &got)
	if err != nil || src != SourceEmbedded || got.N != 1 {
		t.Fatalf("got src=%q err=%v blob=%+v", src, err, got)
	}
}

func TestPutGetRoundTrip(t *testing.T) {
	c := newFakeCache()
	s := testStore(c)
	want := blob{Source: "live", AsOf: "2026-10", N: 7}
	if err := s.Put(context.Background(), "demo", want); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(CacheKey("demo"), "ref:") || c.ttl != DefaultTTL {
		t.Errorf("key %q ttl %v, want ref: prefix and %v", CacheKey("demo"), c.ttl, DefaultTTL)
	}
	var got blob
	src, err := s.Get(context.Background(), "demo", &got)
	if err != nil || src != SourceLive || got != want {
		t.Fatalf("got src=%q err=%v blob=%+v, want live %+v", src, err, got, want)
	}
}

func TestPutFailureStillServesEmbedded(t *testing.T) {
	c := newFakeCache()
	c.setErr = errors.New("OOM command not allowed")
	s := testStore(c)
	if err := s.Put(context.Background(), "demo", blob{N: 2}); err == nil {
		t.Fatal("Put with a failing cache returned nil")
	}
	var got blob
	if src, err := s.Get(context.Background(), "demo", &got); err != nil || src != SourceEmbedded {
		t.Fatalf("got src=%q err=%v, want embedded", src, err)
	}
}

func TestPutNilCacheErrors(t *testing.T) {
	if err := testStore(nil).Put(context.Background(), "demo", blob{}); err == nil {
		t.Fatal("Put with no cache returned nil")
	}
}

func TestGetMissingEmbeddedErrors(t *testing.T) {
	c := newFakeCache()
	c.getErr = errors.New("down")
	var got blob
	if _, err := testStore(c).Get(context.Background(), "absent", &got); err == nil {
		t.Fatal("expected an error, got a silent empty value")
	}
}

type countryRow struct {
	City     string  `json:"city"`
	Country  string  `json:"country"`
	Code     string  `json:"code"`
	Currency *string `json:"currency"`
	EuroArea bool    `json:"euroArea"`
	Sensor   *string `json:"sensor"`
}

type countries struct {
	Source string       `json:"source"`
	AsOf   string       `json:"asOf"`
	Rows   []countryRow `json:"rows"`
}

// The table is read by /atlas, /market, the home band and ⌘K (bearing.md #95).
// The 23 Atlas rows live in the mockups, the docs and this table, and nothing
// else would notice them drifting.
func TestCountryTable(t *testing.T) {
	st := New(nil, Embedded(), nil)
	all, err := st.Countries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	atlasRows, err := st.AtlasCountries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 25 || len(atlasRows) != 23 {
		t.Fatalf("table has %d rows and %d Atlas rows, want 25 and 23 (Colombia and Peru carry a sensor but no Atlas row)", len(all), len(atlasRows))
	}

	universe := map[string]bool{}
	for _, s := range source.SweepUniverse {
		universe[s.Symbol] = true
	}
	regions := map[string]bool{"americas": true, "europe": true, "asia-pacific": true, "middle-east": true, "africa": true}
	slugs, codes := map[string]bool{}, map[string]bool{}
	var sensors, atlasSensors int
	thin := map[string]bool{}
	for _, c := range all {
		if slugs[c.Slug] || codes[c.Code] || c.Slug == "" {
			t.Errorf("duplicate or empty slug/code: %q %q", c.Slug, c.Code)
		}
		slugs[c.Slug], codes[c.Code] = true, true
		if c.EuroArea != (c.Currency == nil && c.Atlas) {
			t.Errorf("%s: euroArea=%v but currency=%v; euro-area rows carry a null currency", c.Slug, c.EuroArea, c.Currency)
		}
		if !regions[c.Region] {
			t.Errorf("%s: unknown region %q", c.Slug, c.Region)
		}
		if c.Atlas != (c.City != nil) {
			t.Errorf("%s: atlas=%v but city=%v", c.Slug, c.Atlas, c.City)
		}
		if c.Sensor != nil {
			sensors++
			if c.Atlas {
				atlasSensors++
			}
			if !universe[*c.Sensor] {
				t.Errorf("%s: sensor %s is not in source.SweepUniverse", c.Slug, *c.Sensor)
			}
		}
		if c.Liquidity != nil {
			if *c.Liquidity != "thin" || c.Sensor == nil {
				t.Errorf("%s: liquidity %q", c.Slug, *c.Liquidity)
			} else {
				thin[*c.Sensor] = true
			}
		}
		checkNewsConfig(t, c)
	}
	if sensors != 13 || atlasSensors != 11 {
		t.Errorf("%d sensors, %d with an Atlas row; want 13 and 11", sensors, atlasSensors)
	}
	if len(thin) != 4 || !thin["KB"] || !thin["BCH"] || !thin["TKC"] || !thin["TLK"] {
		t.Errorf("thin listings = %v, want KB, BCH, TKC and TLK", thin)
	}
	for _, off := range []string{"colombia", "peru"} {
		for _, c := range atlasRows {
			if c.Slug == off {
				t.Errorf("%s has an Atlas row but no OECD series", off)
			}
		}
	}
	// The mock's row ids are the Atlas slugs.
	for _, want := range []string{"italy", "korea", "turkiye", "united-kingdom", "south-africa", "new-zealand", "united-states"} {
		if !slugs[want] {
			t.Errorf("no row with slug %q", want)
		}
	}
}

// Phase 10 gate: every value carries its source and as-of.
func TestEmbeddedValuesCarrySourceAndAsOf(t *testing.T) {
	var c countries
	if _, err := New(nil, Embedded(), nil).Get(context.Background(), "countries", &c); err != nil {
		t.Fatal(err)
	}
	if c.Source == "" || c.AsOf == "" {
		t.Errorf("countries.json: source=%q asOf=%q", c.Source, c.AsOf)
	}

}

func TestEmbeddedHasOnlyKnownFiles(t *testing.T) {
	m, err := fs.Glob(Embedded(), "*.json")
	if err != nil || len(m) < 2 {
		t.Fatalf("glob=%v err=%v", m, err)
	}
}

// checkNewsConfig pins Phase 15 step 5's fields: a sensor's row carries both
// news phrases, a central bank feed or the reason it has none, and the filer's
// EDGAR country; a row without a sensor carries none of them.
func checkNewsConfig(t *testing.T, c Country) {
	t.Helper()
	if c.Sensor == nil {
		if c.EconomyQuery != nil || c.SectorQuery != nil || c.CentralBankFeed != nil || c.CentralBankFeedReason != nil || c.EdgarCountry != nil || c.EdgarOverride {
			t.Errorf("%s: no sensor, but news configuration is set", c.Slug)
		}
		return
	}
	if c.EconomyQuery == nil || *c.EconomyQuery == "" || c.SectorQuery == nil || *c.SectorQuery == "" {
		t.Errorf("%s: a sensor needs an economy and a sector query", c.Slug)
	}
	if (c.CentralBankFeed == nil) == (c.CentralBankFeedReason == nil) {
		t.Errorf("%s: want exactly one of a central bank feed and the reason it has none", c.Slug)
	}
	if f := c.CentralBankFeed; f != nil {
		if !strings.HasPrefix(f.URL, "https://") || len(f.Language) != 2 {
			t.Errorf("%s: feed %+v", c.Slug, f)
		}
		switch f.Format {
		case "rss2", "rss1", "atom":
		default:
			t.Errorf("%s: feed format %q", c.Slug, f.Format)
		}
	}
	if c.EdgarCountry == nil || *c.EdgarCountry == "" {
		t.Errorf("%s: a sensor needs its EDGAR country", c.Slug)
	}
	if c.EdgarOverride != (*c.Sensor == "BAP") {
		t.Errorf("%s: edgarOverride = %v; only BAP files in one country and senses another", c.Slug, c.EdgarOverride)
	}
}

func TestNewsConfigCounts(t *testing.T) {
	all, err := New(nil, Embedded(), nil).Countries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	feeds := 0
	var none []string
	for _, c := range all {
		if c.Sensor == nil {
			continue
		}
		if c.CentralBankFeed != nil {
			feeds++
		} else {
			none = append(none, *c.Sensor)
		}
	}
	slices.Sort(none)
	if feeds != 8 || !slices.Equal(none, []string{"AMX", "BAP", "BCH", "KB", "TLK"}) {
		t.Errorf("%d feeds, none for %v; want 8, and none for AMX BAP BCH KB TLK (phase8.md)", feeds, none)
	}
}

func TestGlobalSensorsCarryATopic(t *testing.T) {
	sensors, err := New(nil, Embedded(), nil).Sensors(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range sensors {
		switch s.Group {
		case "global":
			if s.NewsTopic == nil || *s.NewsTopic == "" {
				t.Errorf("%s: a global sensor needs a topic", s.Symbol)
			}
			if s.NewsTopicProvisional != (s.Symbol == "CAT") {
				t.Errorf("%s: provisional = %v; only CAT's wording is provisional (research/news-queries.md)", s.Symbol, s.NewsTopicProvisional)
			}
		default:
			if s.NewsTopic != nil {
				t.Errorf("%s: a country sensor uses its country's queries, not a topic", s.Symbol)
			}
		}
	}
	// None of it is served.
	b, err := json.Marshal(sensors)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "newsTopic") || strings.Contains(string(b), "chip stocks") {
		t.Error("the topic is configuration and must not be served by GET /sensors")
	}
}

func TestStale(t *testing.T) {
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	built := func(age time.Duration) string {
		return `{"source":"live","asOf":"` + now.Add(-age).Format(time.RFC3339) + `","n":2}`
	}
	at := func(b blob) string { return b.AsOf }
	cases := []struct {
		name  string
		cache func() *fakeCache
		want  bool
	}{
		{"fresh live", func() *fakeCache { c := newFakeCache(); c.data[CacheKey("demo")] = built(time.Hour); return c }, false},
		{"exactly the max age", func() *fakeCache { c := newFakeCache(); c.data[CacheKey("demo")] = built(24 * time.Hour); return c }, false},
		{"older than the max age", func() *fakeCache { c := newFakeCache(); c.data[CacheKey("demo")] = built(25 * time.Hour); return c }, true},
		{"embedded fallback counts as missing", newFakeCache, true},
		{"cache down", func() *fakeCache { c := newFakeCache(); c.getErr = errors.New("down"); return c }, true},
		{"unparsable build time", func() *fakeCache {
			c := newFakeCache()
			c.data[CacheKey("demo")] = `{"source":"live","asOf":"yesterday","n":2}`
			return c
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Stale(context.Background(), testStore(tc.cache()), "demo", now, 24*time.Hour, at); got != tc.want {
				t.Errorf("Stale = %v, want %v", got, tc.want)
			}
		})
	}
}
