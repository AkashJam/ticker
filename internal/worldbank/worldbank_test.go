package worldbank

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/store"
)

const sampleBody = `[{"page":1,"pages":1,"per_page":1000,"total":3},[
 {"country":{"id":"IT"},"date":"2025","value":6.391},
 {"country":{"id":"US"},"date":"2025","value":null},
 {"country":{"id":"US"},"date":"2024","value":4.1}]]`

func TestParseDropsNullValues(t *testing.T) {
	pg, err := parse([]byte(sampleBody), "SL.UEM.TOTL.ZS")
	if err != nil || len(pg.obs) != 2 || pg.pages != 1 {
		t.Fatalf("page = %+v err = %v", pg, err)
	}
	if pg.obs[0] != (Obs{Code: "IT", Year: 2025, Value: 6.391}) || pg.obs[1].Year != 2024 {
		t.Errorf("obs = %+v", pg.obs)
	}
}

func TestParseErrors(t *testing.T) {
	for _, body := range []string{``, `[`, `null`, `[{"page":1}]`} {
		if _, err := parse([]byte(body), "X"); !errors.Is(err, ErrBadBody) {
			t.Errorf("%q: err = %v, want ErrBadBody (retryable)", body, err)
		}
	}
	_, err := parse([]byte(`[{"message":[{"id":"120","key":"Invalid value","value":"The provided parameter value is not valid"}]}]`), "X")
	var ae *APIError
	if !errors.As(err, &ae) || errors.Is(err, ErrBadBody) {
		t.Errorf("err = %v, want a non-retryable APIError", err)
	}
}

func TestClientIndicatorPagesAndQuery(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if !strings.HasPrefix(r.URL.Path, "/country/IT;US/indicator/FP.CPI.TOTL") {
			t.Errorf("path = %s", r.URL.Path)
		}
		if r.URL.Query().Get("mrnev") != "1" || r.URL.Query().Get("format") != "json" {
			t.Errorf("query = %s", r.URL.RawQuery)
		}
		switch r.URL.Query().Get("page") {
		case "1":
			_, _ = w.Write([]byte(`[{"page":1,"pages":2},[{"country":{"id":"IT"},"date":"2024","value":1}]]`))
		default:
			_, _ = w.Write([]byte(`[{"page":2,"pages":2},[{"country":{"id":"US"},"date":"2024","value":2}]]`))
		}
	}))
	t.Cleanup(srv.Close)
	c := NewClient()
	c.baseURL = srv.URL

	obs, err := c.Indicator(context.Background(), []string{"IT", "US"}, "FP.CPI.TOTL", url.Values{"mrnev": {"1"}})
	if err != nil || len(obs) != 2 || hits.Load() != 2 {
		t.Fatalf("obs = %v err = %v after %d requests, want both pages", obs, err, hits.Load())
	}
}

func TestClientStatusAndNoData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "LIMITED"):
			w.Header().Set("Retry-After", "4")
			w.WriteHeader(http.StatusTooManyRequests)
		case strings.Contains(r.URL.Path, "EMPTY"):
			_, _ = w.Write([]byte(`[{"page":1,"pages":1},null]`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	c := NewClient()
	c.baseURL = srv.URL

	var se *StatusError
	if _, err := c.Indicator(context.Background(), []string{"IT"}, "LIMITED", nil); !errors.As(err, &se) || !se.Retryable() || se.RetryAfter != 4*time.Second {
		t.Errorf("err = %v, want a retryable 429 with Retry-After", err)
	}
	if _, err := c.Indicator(context.Background(), []string{"IT"}, "EMPTY", nil); !errors.Is(err, ErrNoData) {
		t.Errorf("err = %v, want ErrNoData", err)
	}
	if _, err := c.Indicator(context.Background(), []string{"IT"}, "OTHER", nil); !errors.As(err, &se) || se.Retryable() {
		t.Errorf("err = %v, want a non-retryable 404", err)
	}
}

// --- refresher ---

type fakeAPI struct {
	data  map[string][]Obs
	err   map[string]error
	flaky map[string]int // ErrBadBody this many times first
	calls map[string]int
}

func (f *fakeAPI) Indicator(_ context.Context, _ []string, id string, _ url.Values) ([]Obs, error) {
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[id]++
	if f.calls[id] <= f.flaky[id] {
		return nil, ErrBadBody
	}
	if err := f.err[id]; err != nil {
		return nil, err
	}
	return f.data[id], nil
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

func series(code string, from, to int) []Obs {
	var o []Obs
	for y := from; y <= to; y++ {
		o = append(o, Obs{Code: code, Year: y, Value: 100 + float64(y-from)})
	}
	return o
}

// goodData: Italy is current to 2025; the US lags a year on consumption and CPI.
func goodData() map[string][]Obs {
	d := map[string][]Obs{
		"FP.CPI.TOTL":    append(series("IT", 2010, 2025), series("US", 2010, 2024)...),
		"NE.CON.PRVT.CD": {{"IT", 2025, 1.2e12}, {"US", 2024, 2.0e13}},
		// A different 2025 population catches pairing with the wrong year.
		"SP.POP.TOTL":       {{"IT", 2024, 6.0e7}, {"IT", 2025, 6.0e7}, {"US", 2024, 3.4e8}, {"US", 2025, 9.9e8}},
		"NY.GNP.PCAP.PP.CD": {{"IT", 2025, 62867.4}, {"US", 2025, 90000.2}},
		"SL.UEM.TOTL.ZS":    {{"IT", 2025, 6.391}, {"US", 2025, 4.04}},
		"SP.DYN.LE00.IN":    {{"IT", 2024, 84.04}, {"US", 2024, 79.3}},
		"FP.CPI.TOTL.ZG":    {{"IT", 2025, 1.7}, {"US", 2024, 2.9}},
		"NV.AGR.TOTL.ZS":    {{"IT", 2025, 1.9}, {"US", 2025, 1}},
		"NV.IND.TOTL.ZS":    {{"IT", 2025, 21.3}, {"US", 2025, 17}},
		"NV.SRV.TOTL.ZS":    {{"IT", 2025, 66}, {"US", 2025, 77}},
		"NV.IND.MANF.ZS":    {{"IT", 2025, 14.9}, {"US", 2025, 10}},
	}
	return d
}

const ecbJSON = `{"currencies":{"USD":{"annualAverage":{"2024":1.1,"2025":1.25}}}}`

func testRefresher(api IndicatorSource) (*Refresher, *memCache) {
	cache := &memCache{data: map[string]string{}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fsys := fstest.MapFS{
		"countries.json": {Data: []byte(`{"rows":[{"code":"IT","atlas":true},{"code":"US","atlas":true},{"code":"CO","atlas":false}]}`)},
		"ecb.json":       {Data: []byte(ecbJSON)},
	}
	r := NewRefresher(api, reference.New(cache, fsys, log), log)
	r.Sleep = func(context.Context, time.Duration) error { return nil }
	r.Now = func() time.Time { return time.Date(2026, 10, 12, 6, 0, 0, 0, time.UTC) }
	return r, cache
}

func f(v *float64) float64 {
	if v == nil {
		return -1
	}
	return *v
}

func TestBuildAssemblesEveryMeasure(t *testing.T) {
	r, _ := testRefresher(&fakeAPI{data: goodData()})
	ds, err := r.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	it, us := ds.Rows["IT"], ds.Rows["US"]

	// 1.2e12 / 6e7 people = $20,000 a year; at 1.25 $/€ that is €16,000; ÷ 12.
	if got := f(it.EverydaySpendingEur.Value); got != 1333 || it.EverydaySpendingEur.AsOf != "2025" {
		t.Errorf("IT spending = %v (%s), want 1333 (2025)", got, it.EverydaySpendingEur.AsOf)
	}
	// The US consumption is for 2024, so population and rate are 2024's.
	if got := f(us.EverydaySpendingEur.Value); got != 4456 || us.EverydaySpendingEur.AsOf != "2024" {
		t.Errorf("US spending = %v (%s), want 4456 (2024); the 2025 population must not be used", got, us.EverydaySpendingEur.AsOf)
	}
	if got := f(it.GniPpp.Value); got != 62867 || it.GniPpp.Unit != "USD PPP/person" || it.GniPpp.Source != "World Bank NY.GNP.PCAP.PP.CD" {
		t.Errorf("IT income = %+v", it.GniPpp)
	}
	if f(it.Unemployment.Value) != 6.4 || f(it.LifeExpectancy.Value) != 84 || it.LifeExpectancy.AsOf != "2024" {
		t.Errorf("IT unemployment %v, life expectancy %v (%s)", f(it.Unemployment.Value), f(it.LifeExpectancy.Value), it.LifeExpectancy.AsOf)
	}
	if us.Inflation.AsOf != "2024" || f(it.Composition.Services.Value) != 66 {
		t.Errorf("US inflation asOf %s, IT services %v", us.Inflation.AsOf, f(it.Composition.Services.Value))
	}
	// Each country keeps its own end; the cut at the common year is the route's.
	if it.Prices.AsOf != "2025" || us.Prices.AsOf != "2024" || it.Prices.Series.Step != 12 || it.Prices.Series.Start != "2010-01" {
		t.Errorf("prices: IT %s, US %s, series %+v", it.Prices.AsOf, us.Prices.AsOf, it.Prices.Series)
	}
	if w := it.Prices.Series.Window(2010, 2024); w == nil || *w != 14 {
		t.Errorf("IT prices 2010-24 = %v, want +14 (114 against 100)", w)
	}
}

func TestEveryValueCarriesSourceAndAsOf(t *testing.T) {
	r, _ := testRefresher(&fakeAPI{data: goodData()})
	ds, err := r.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for code, row := range ds.Rows {
		for name, v := range map[string]Value{
			"spending": row.EverydaySpendingEur, "income": row.GniPpp, "unemployment": row.Unemployment,
			"life": row.LifeExpectancy, "inflation": row.Inflation,
			"agri": row.Composition.Agriculture, "industry": row.Composition.Industry,
			"services": row.Composition.Services, "manufacturing": row.Composition.Manufacturing,
		} {
			if v.Source == "" || v.AsOf == "" || v.Value == nil {
				t.Errorf("%s %s: %+v", code, name, v)
			}
		}
		if row.Prices.Source == "" || row.Prices.AsOf == "" {
			t.Errorf("%s prices: %+v", code, row.Prices)
		}
	}
}

func TestMissingValuesAreNullWithAReason(t *testing.T) {
	d := goodData()
	d["NV.SRV.TOTL.ZS"] = []Obs{{"US", 2025, 77}} // no Italian services share
	d["NE.CON.PRVT.CD"] = []Obs{{"IT", 2025, 1.2e12}, {"US", 2023, 1}}
	r, _ := testRefresher(&fakeAPI{data: d})
	ds, err := r.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if v := ds.Rows["IT"].Composition.Services; v.Value != nil || v.Reason != "not published" || v.Source == "" || v.AsOf == "" {
		t.Errorf("IT services = %+v, want null with a reason, a source and an as-of", v)
	}
	// US consumption is for 2023, which has neither a population nor an ECB rate here.
	if v := ds.Rows["US"].EverydaySpendingEur; v.Value != nil || !strings.HasPrefix(v.Reason, "no population for 2023") {
		t.Errorf("US spending = %+v", v)
	}
}

func TestSpendingNeedsAnECBRateForTheYear(t *testing.T) {
	v := spending(Obs{Code: "IT", Year: 2023, Value: 1e12}, map[int]float64{2023: 6e7}, map[string]float64{"2024": 1.1}, "2026-10-12")
	if v.Value != nil || v.Reason != "no ECB rate for 2023" {
		t.Errorf("spending = %+v", v)
	}
}

func TestPricesMustStartIn2010AndHaveNoGap(t *testing.T) {
	d := goodData()
	d["FP.CPI.TOTL"] = append(series("IT", 2010, 2025), series("US", 2011, 2024)...)
	r, _ := testRefresher(&fakeAPI{data: d})
	if _, err := r.Build(context.Background()); err == nil || !strings.Contains(err.Error(), "US") {
		t.Errorf("err = %v, want the US named", err)
	}

	d["FP.CPI.TOTL"] = append(series("IT", 2010, 2025), append(series("US", 2010, 2015), series("US", 2017, 2024)...)...)
	if _, err := r.Build(context.Background()); err == nil {
		t.Error("a CPI series with a missing year was accepted")
	}
}

func TestRefreshIsAllOrNothing(t *testing.T) {
	api := &fakeAPI{data: goodData(), err: map[string]error{"SP.DYN.LE00.IN": &APIError{Message: "bad"}}}
	r, cache := testRefresher(api)
	err := r.Refresh(context.Background())
	if err == nil || !strings.Contains(err.Error(), "SP.DYN.LE00.IN") {
		t.Fatalf("err = %v, want the failed indicator named", err)
	}
	if len(cache.data) != 0 {
		t.Error("a partial fetch was stored")
	}
	if api.calls["SP.DYN.LE00.IN"] != 1 {
		t.Errorf("an API error was tried %d times", api.calls["SP.DYN.LE00.IN"])
	}
}

func TestEmptyBodiesAreRetried(t *testing.T) {
	api := &fakeAPI{data: goodData(), flaky: map[string]int{"SL.UEM.TOTL.ZS": 2}}
	r, cache := testRefresher(api)
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("two empty bodies then success should succeed: %v", err)
	}
	if api.calls["SL.UEM.TOTL.ZS"] != 3 {
		t.Errorf("calls = %d, want 3", api.calls["SL.UEM.TOTL.ZS"])
	}
	if _, ok := cache.data[reference.CacheKey(DatasetName)]; !ok {
		t.Error("dataset was not stored")
	}

	api = &fakeAPI{data: goodData(), flaky: map[string]int{"SL.UEM.TOTL.ZS": 99}}
	r, _ = testRefresher(api)
	if err := r.Refresh(context.Background()); err == nil || api.calls["SL.UEM.TOTL.ZS"] != r.Attempts {
		t.Errorf("err = %v after %d calls, want failure after %d", err, api.calls["SL.UEM.TOTL.ZS"], r.Attempts)
	}
}

func TestStale(t *testing.T) {
	r, _ := testRefresher(&fakeAPI{data: goodData()})
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
	r.Now = func() time.Time { return time.Date(2026, 10, 14, 6, 0, 0, 0, time.UTC) }
	if !r.Stale(ctx, 24*time.Hour) {
		t.Error("a two-day-old dataset is not stale")
	}
}

// The change block's spending: consumption ÷ population of the same year ÷ 12,
// in the country's own currency, from 2010 with no gap.
func TestSpendingSeriesIsLocalPerPersonPerMonth(t *testing.T) {
	d := goodData()
	var cons, pop []Obs
	for y := 2010; y <= 2025; y++ {
		cons = append(cons, Obs{"IT", y, 1.2e12 + float64(y-2010)*1.2e10})
		pop = append(pop, Obs{"IT", y, 6.0e7})
	}
	d["NE.CON.PRVT.CN"] = cons
	d["SP.POP.TOTL"] = append(pop, Obs{"US", 2024, 3.4e8})
	r, _ := testRefresher(&fakeAPI{data: d})
	ds, err := r.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sp := ds.Rows["IT"].Spending
	if sp.Series == nil || sp.Series.Start != "2010-01" || sp.Series.Step != 12 || len(sp.Series.Values) != 16 || sp.AsOf != "2025" {
		t.Fatalf("IT spending %+v", sp)
	}
	// 1.2e12 / 6e7 = 20,000 a year; ÷ 12.
	if v := sp.Series.Values[0]; v < 1666.6 || v > 1666.7 {
		t.Errorf("IT 2010 = %v, want 1666.67", v)
	}
	// No consumption for the US: null with a reason, and the build still succeeds.
	if us := ds.Rows["US"].Spending; us.Series != nil || us.Reason == "" {
		t.Errorf("US spending %+v, want nil with a reason", us)
	}
}

func TestSpendingSeriesNeedsPopulationEveryYear(t *testing.T) {
	cons := []Obs{{"IT", 2010, 1}, {"IT", 2011, 1}}
	sp := spendingSeries(cons, map[int]float64{2010: 1}, "2026-10-29")
	if sp.Series != nil || sp.Reason != "no population for 2011" {
		t.Errorf("spending %+v", sp)
	}
	sp = spendingSeries([]Obs{{"IT", 2011, 1}}, map[int]float64{2011: 1}, "2026-10-29")
	if sp.Series != nil || sp.Reason == "" {
		t.Errorf("a series that starts after 2010 must not be served: %+v", sp)
	}
}
