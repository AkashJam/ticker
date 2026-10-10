package reference

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
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

// The twenty-three live in the mockups, the docs and this snapshot, and
// nothing else would notice them drifting.
func TestEmbeddedCountries(t *testing.T) {
	var c countries
	src, err := New(nil, Embedded(), nil).Get(context.Background(), "countries", &c)
	if err != nil || src != SourceEmbedded {
		t.Fatalf("src=%q err=%v", src, err)
	}
	if len(c.Rows) != 23 {
		t.Fatalf("countries.json has %d rows, want 23", len(c.Rows))
	}
	universe := map[string]bool{}
	for _, s := range source.SweepUniverse {
		universe[s.Symbol] = true
	}
	codes, cities, sensors := map[string]bool{}, map[string]bool{}, 0
	for _, r := range c.Rows {
		if codes[r.Code] || cities[r.City] {
			t.Errorf("duplicate row %s/%s", r.City, r.Code)
		}
		codes[r.Code], cities[r.City] = true, true
		if r.EuroArea != (r.Currency == nil) {
			t.Errorf("%s: euroArea=%v but currency=%v; euro-area rows carry a null currency", r.City, r.EuroArea, r.Currency)
		}
		if r.Sensor != nil {
			sensors++
			if !universe[*r.Sensor] {
				t.Errorf("%s: sensor %s is not in source.SweepUniverse", r.City, *r.Sensor)
			}
		}
	}
	if sensors != 11 {
		t.Errorf("%d rows carry a sensor, want 11 (twelve say they have none)", sensors)
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

	var b struct {
		Rows []struct {
			City   string  `json:"city"`
			Value  float64 `json:"value"`
			Source string  `json:"source"`
			AsOf   string  `json:"asOf"`
		} `json:"rows"`
	}
	if _, err := New(nil, Embedded(), nil).Get(context.Background(), "survey_baselines", &b); err != nil {
		t.Fatal(err)
	}
	if len(b.Rows) != 5 {
		t.Errorf("survey_baselines.json has %d rows, want 5", len(b.Rows))
	}
	for _, r := range b.Rows {
		if r.Value <= 0 || r.Source == "" || r.AsOf == "" {
			t.Errorf("%s: value=%v source=%q asOf=%q", r.City, r.Value, r.Source, r.AsOf)
		}
	}
}

func TestEmbeddedHasOnlyKnownFiles(t *testing.T) {
	m, err := fs.Glob(Embedded(), "*.json")
	if err != nil || len(m) < 2 {
		t.Fatalf("glob=%v err=%v", m, err)
	}
}
