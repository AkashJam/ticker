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

func TestLastCompleteQuarterAndCommonEnd(t *testing.T) {
	// A monthly series that stops in August has not finished Q3.
	if q := LastCompleteQuarter([]Obs{{Date: d(2026, 8)}}, Monthly); q != (Quarter{2026, 2}) {
		t.Errorf("August monthly = %v, want 2026-Q2", q)
	}
	if q := LastCompleteQuarter([]Obs{{Date: d(2026, 9)}}, Monthly); q != (Quarter{2026, 3}) {
		t.Errorf("September monthly = %v, want 2026-Q3", q)
	}
	if q := LastCompleteQuarter([]Obs{{Date: d(2026, 2)}}, Monthly); q != (Quarter{2025, 4}) {
		t.Errorf("February monthly = %v, want 2025-Q4", q)
	}
	if q := LastCompleteQuarter([]Obs{{Date: d(2026, 1)}}, Quarterly); q != (Quarter{2026, 1}) {
		t.Errorf("January quarterly = %v, want 2026-Q1", q)
	}
	// Japan's BIS series runs a quarter behind and sets the end for everyone.
	if got := CommonEnd([]Quarter{{2026, 1}, {2025, 4}, {2026, 1}}); got.String() != "2025-Q4" {
		t.Errorf("common end = %v", got)
	}
}

func TestWindowAndTrail(t *testing.T) {
	// Quarterly: 100 in Jan 2010, 150 in Jan 2014, 200 in Oct 2025.
	h := []Obs{{d(2010, 1), 100}, {d(2014, 1), 150}, {d(2025, 10), 200}}
	end := Quarter{2025, 4}
	if v := Window(h, Quarterly, 2010, end); v == nil || *v != 100 {
		t.Errorf("2010 window = %v, want +100", v)
	}
	if v := Window(h, Quarterly, 2016, end); v != nil {
		t.Errorf("2016 window = %v, want nil (no January 2016 observation)", *v)
	}
	// Monthly: the end point is the last month of the quarter, December.
	s := []Obs{{d(2010, 1), 50}, {d(2025, 12), 100}}
	if v := Window(s, Monthly, 2010, end); v == nil || *v != 100 {
		t.Errorf("monthly 2010 window = %v, want +100", v)
	}
	tr := Trail(h, Quarterly, []int{2014, 2018}, end)
	if len(tr) != 3 || tr[0].At != "2014" || *tr[0].Change != 50 || tr[1].Change != nil || tr[2].At != "2025-Q4" || *tr[2].Change != 100 {
		t.Errorf("trail = %+v", tr)
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

func goodSeries() map[string][]Obs {
	m := map[string][]Obs{}
	for _, cc := range []string{"IT", "JP"} {
		m[SharesID(cc)] = []Obs{{d(2010, 1), 100}, {d(2021, 1), 200}, {d(2025, 12), 300}, {d(2026, 8), 320}}
		m[HousingID(cc)] = []Obs{{d(2010, 1), 100}, {d(2021, 1), 100}, {d(2025, 10), 110}, {d(2026, 1), 120}}
	}
	// Japan's BIS series runs one quarter behind.
	m[HousingID("JP")] = m[HousingID("JP")][:3]
	return m
}

func TestRefreshBuildsCommonEndAndStores(t *testing.T) {
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
	if ds.End != "2025-Q4" {
		t.Errorf("end = %s, want 2025-Q4 (Japan's housing lags a quarter)", ds.End)
	}
	it := ds.Rows["IT"]
	if v := it.Shares.Change["2010"]; v == nil || *v != 200 {
		t.Errorf("IT shares 2010 = %v, want +200", v)
	}
	if v := it.Housing.Change["2021"]; v == nil || *v != 10 {
		t.Errorf("IT housing 2021 = %v, want +10", v)
	}
	for _, m := range []Measure{it.Shares, it.Housing} {
		if m.Source == "" || m.AsOf != "2025-Q4" {
			t.Errorf("measure lacks source or as-of: %+v", m)
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
