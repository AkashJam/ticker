package fred

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/store"
)

func d(y, m int) time.Time { return time.Date(y, time.Month(m), 1, 0, 0, 0, 0, time.UTC) }

func TestParseCSVDropsMissingAndChecksHeader(t *testing.T) {
	obs, err := parseCSV(strings.NewReader("observation_date,QITR628BIS\n2025-10-01,75.5\n2026-01-01,.\n2026-04-01,76\n"), "QITR628BIS")
	if err != nil || len(obs) != 2 || obs[1].Value != 76 || !obs[1].Date.Equal(d(2026, 4)) {
		t.Fatalf("obs=%v err=%v", obs, err)
	}
	if _, err := parseCSV(strings.NewReader("observation_date,OTHER\n2025-10-01,1\n"), "QITR628BIS"); err == nil {
		t.Error("a header for a different series was accepted")
	}
	if _, err := parseCSV(strings.NewReader("observation_date,X\nnot-a-date,1\n"), "X"); err == nil {
		t.Error("a bad date was accepted")
	}
}

func TestClientSeriesAndStatusErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("id") {
		case "GOOD":
			_, _ = w.Write([]byte("observation_date,GOOD\n2026-01-01,1.5\n"))
		case "EMPTY":
			_, _ = w.Write([]byte("observation_date,EMPTY\n2026-01-01,.\n"))
		case "LIMITED":
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)
	c := NewClient()
	c.baseURL = srv.URL

	if obs, err := c.Series(context.Background(), "GOOD"); err != nil || len(obs) != 1 {
		t.Fatalf("obs=%v err=%v", obs, err)
	}
	if _, err := c.Series(context.Background(), "EMPTY"); !errors.Is(err, ErrNoData) {
		t.Errorf("err=%v, want ErrNoData", err)
	}
	var se *StatusError
	if _, err := c.Series(context.Background(), "LIMITED"); !errors.As(err, &se) || !se.Retryable() || se.RetryAfter != 7*time.Second {
		t.Errorf("err=%v, want retryable 429 with Retry-After 7s", err)
	}
	if _, err := c.Series(context.Background(), "NOPE"); !errors.As(err, &se) || se.Retryable() {
		t.Errorf("err=%v, want non-retryable 400", err)
	}
}

func TestSeriesIDs(t *testing.T) {
	if SharesID("GB") != "SPASTT01GBM661N" || HousingID("IT") != "QITR628BIS" {
		t.Errorf("ids: %s %s", SharesID("GB"), HousingID("IT"))
	}
}

func TestToSeries(t *testing.T) {
	var obs []Obs
	for m := 0; m < 30; m++ { // October 2009 to March 2012, monthly
		obs = append(obs, Obs{Date: d(2009, 10).AddDate(0, m, 0), Value: float64(m)})
	}
	s, err := ToSeries(obs, 1)
	if err != nil {
		t.Fatal(err)
	}
	if s.Start != "2010-01" || len(s.Values) != 27 || s.Values[0] != 3 || s.AsOf() != "2012-03" {
		t.Errorf("series = %+v asOf %s, want it to start at 2010-01 (the third observation)", s, s.AsOf())
	}

	q := []Obs{{d(2010, 1), 1}, {d(2010, 4), 2}, {d(2010, 7), 3}, {d(2010, 10), 4}}
	if s, err := ToSeries(q, 3); err != nil || s.AsOf() != "2010-Q4" {
		t.Errorf("quarterly = %+v err = %v", s, err)
	}
}

func TestToSeriesRejectsWhatItCannotServeHonestly(t *testing.T) {
	if _, err := ToSeries([]Obs{{d(2010, 4), 1}, {d(2010, 7), 2}}, 3); err == nil {
		t.Error("a series that starts after January 2010 was accepted")
	}
	if _, err := ToSeries([]Obs{{d(2010, 1), 1}, {d(2010, 2), 2}, {d(2010, 4), 3}}, 1); err == nil {
		t.Error("a series with a gap was accepted")
	}
	if _, err := ToSeries([]Obs{{d(2009, 1), 1}}, 1); err == nil {
		t.Error("a series that ends before 2010 was accepted")
	}
}

// --- refresher ---

type fakeSource struct {
	series map[string][]Obs
	err    map[string]error
	calls  map[string]int
}

func (f *fakeSource) Series(_ context.Context, id string) ([]Obs, error) {
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[id]++
	if err := f.err[id]; err != nil {
		return nil, err
	}
	return f.series[id], nil
}

type memCache struct{ data map[string]string }

func (m *memCache) CacheGet(_ context.Context, k string) (string, error) {
	if v, ok := m.data[k]; ok {
		return v, nil
	}
	return "", store.ErrCacheMiss
}
func (m *memCache) CacheSet(_ context.Context, k, v string, _ time.Duration) error {
	m.data[k] = v
	return nil
}

const twoCountries = `{"source":"t","asOf":"t","rows":[{"code":"IT"},{"code":"JP"}]}`

func testRefresher(src SeriesSource) (*Refresher, *memCache) {
	cache := &memCache{data: map[string]string{}}
	fsys := fstest.MapFS{"countries.json": {Data: []byte(twoCountries)}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := NewRefresher(src, reference.New(cache, fsys, log), log)
	r.Sleep = func(context.Context, time.Duration) error { return nil }
	r.Now = func() time.Time { return time.Date(2026, 10, 10, 6, 0, 0, 0, time.UTC) }
	return r, cache
}

// monthlyObs and quarterlyObs build contiguous series from January 2010.
func monthlyObs(n int, base float64) []Obs {
	var o []Obs
	for i := 0; i < n; i++ {
		o = append(o, Obs{Date: d(2010, 1).AddDate(0, i, 0), Value: base + float64(i)})
	}
	return o
}

func quarterlyObs(n int, base float64) []Obs {
	var o []Obs
	for i := 0; i < n; i++ {
		o = append(o, Obs{Date: d(2010, 1).AddDate(0, 3*i, 0), Value: base + float64(i)})
	}
	return o
}

func goodSeries() map[string][]Obs {
	m := map[string][]Obs{}
	for _, cc := range []string{"IT", "JP"} {
		m[SharesID(cc)] = monthlyObs(200, 100)
		m[HousingID(cc)] = quarterlyObs(60, 100)
	}
	// Japan's BIS series runs one quarter behind.
	m[HousingID("JP")] = m[HousingID("JP")][:59]
	return m
}

func TestRefreshStoresEachCountrysOwnSeries(t *testing.T) {
	r, cache := testRefresher(&fakeSource{series: goodSeries()})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.data[reference.CacheKey(DatasetName)]; !ok {
		t.Fatal("dataset was not stored")
	}
	ds, err := r.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Each series keeps its own end; the cut at the common year is the route's.
	it, jp := ds.Rows["IT"], ds.Rows["JP"]
	if it.Shares.AsOf != "2026-08" || it.Housing.AsOf != "2024-Q4" || jp.Housing.AsOf != "2024-Q3" {
		t.Errorf("asOf: IT shares %s, IT housing %s, JP housing %s", it.Shares.AsOf, it.Housing.AsOf, jp.Housing.AsOf)
	}
	if w := it.Shares.Series.Window(2010, 2024); w == nil || *w != 179 {
		// 100 in January 2010, 279 in December 2024 (index 179).
		t.Errorf("IT shares 2010-2024 = %v, want +179", w)
	}
	for _, m := range []Measure{it.Shares, it.Housing, jp.Shares, jp.Housing} {
		if m.Source == "" || m.AsOf == "" || m.Series.Validate() != nil {
			t.Errorf("measure lacks source, as-of or a valid series: %+v", m)
		}
	}
}

func TestRefreshIsAllOrNothing(t *testing.T) {
	src := &fakeSource{series: goodSeries(), err: map[string]error{HousingID("JP"): &StatusError{Code: 400}}}
	r, cache := testRefresher(src)
	if err := r.Refresh(context.Background()); err == nil || !strings.Contains(err.Error(), HousingID("JP")) {
		t.Fatalf("err = %v, want the failed series named", err)
	}
	if len(cache.data) != 0 {
		t.Error("a partial fetch overwrote the cache")
	}
	if src.calls[HousingID("JP")] != 1 {
		t.Errorf("a non-retryable 400 was tried %d times", src.calls[HousingID("JP")])
	}
}

func TestRefreshRetriesTransientErrors(t *testing.T) {
	src := &fakeSource{series: goodSeries(), err: map[string]error{SharesID("IT"): &StatusError{Code: 503}}}
	r, _ := testRefresher(src)
	if err := r.Refresh(context.Background()); err == nil {
		t.Fatal("expected failure")
	}
	if src.calls[SharesID("IT")] != r.Attempts {
		t.Errorf("attempts = %d, want %d", src.calls[SharesID("IT")], r.Attempts)
	}
}

func TestStale(t *testing.T) {
	r, _ := testRefresher(&fakeSource{series: goodSeries()})
	ctx := context.Background()
	if !r.Stale(ctx, 24*time.Hour) {
		t.Error("an empty cache is not stale")
	}
	if err := r.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Stale(ctx, 24*time.Hour) {
		t.Error("a just-written dataset is stale")
	}
	r.Now = func() time.Time { return time.Date(2026, 10, 12, 6, 0, 0, 0, time.UTC) }
	if !r.Stale(ctx, 24*time.Hour) {
		t.Error("a two-day-old dataset is not stale")
	}
}
