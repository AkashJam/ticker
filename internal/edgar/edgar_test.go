package edgar

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/store"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// --- the normalisation table ---

func TestNormalise(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want string
		ok   bool
	}{
		{"Ontario, Canada", "CA", true},
		{"Canada", "CA", true},
		{"Korea, Republic of", "KR", true},
		{"Turkey", "TR", true},
		{"Bermuda", "BM", true},
		{"  united kingdom ", "GB", true},
		{"NY", "US", true},
		{"CA", "US", true}, // California, not Canada: a two-letter code is a US state
		{"DC", "US", true},
		{"Atlantis", "", false},
		{"", "", false},
	} {
		if got, ok := Normalise(tt.in); got != tt.want || ok != tt.ok {
			t.Errorf("Normalise(%q) = %q, %v; want %q, %v", tt.in, got, ok, tt.want, tt.ok)
		}
	}
}

// Every sensor row's EDGAR string must normalise to its country, or to the
// override's, so the table and the reference file cannot drift apart.
func TestEmbeddedSensorsNormalise(t *testing.T) {
	cs, err := reference.New(nil, reference.Embedded(), quiet()).Countries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, c := range cs {
		if c.Sensor == nil {
			continue
		}
		n++
		iso, ok := Normalise(*c.EdgarCountry)
		if !ok {
			t.Errorf("%s: %q is not in the table", *c.Sensor, *c.EdgarCountry)
			continue
		}
		want := c.Code
		if c.EdgarOverride {
			want = "BM"
		}
		if iso != want {
			t.Errorf("%s: %q normalises to %s, want %s", *c.Sensor, *c.EdgarCountry, iso, want)
		}
	}
	if n != 13 {
		t.Errorf("%d sensor rows, want 13", n)
	}
}

// --- the client ---

func TestNewClientNeedsAnIdentifyingContact(t *testing.T) {
	if _, err := NewClient("   "); !errors.Is(err, ErrNoContact) {
		t.Errorf("empty contact: %v", err)
	}
	if _, err := NewClient("a@b.c\r\nX-Evil: 1"); err == nil {
		t.Error("a contact with a line break must be refused")
	}
	if _, err := NewClient("Jo Example jo@example.org"); err != nil {
		t.Error(err)
	}
}

type recorded struct {
	mu   sync.Mutex
	uas  []string
	hits []string
	at   []time.Time
}

func newServer(t *testing.T, rec *recorded, status int) *Client {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/tickers", func(w http.ResponseWriter, r *http.Request) {
		rec.note(r)
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		_, _ = io.WriteString(w, `{"0":{"cik_str":1067983,"ticker":"ry","title":"Royal Bank"},"1":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}}`)
	})
	mux.HandleFunc("/subs/", func(w http.ResponseWriter, r *http.Request) {
		rec.note(r)
		if strings.Contains(r.URL.Path, "1000275") { // a foreign filer, as EDGAR really answers
			_, _ = io.WriteString(w, `{"cik":"0001000275","name":"ROYAL BANK OF CANADA","tickers":["RY"],"addresses":{"business":{"stateOrCountry":null,"stateOrCountryDescription":null,"isForeignLocation":1,"country":"Ontario, Canada","countryCode":"A6"}}}`)
			return
		}
		_, _ = io.WriteString(w, `{"cik":"0000320193","name":"Apple Inc.","tickers":["AAPL"],"addresses":{"business":{"stateOrCountry":"CA","stateOrCountryDescription":"CA","isForeignLocation":null,"country":null,"countryCode":null}}}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c, err := NewClient("Jo Example jo@example.org")
	if err != nil {
		t.Fatal(err)
	}
	c.tickersURL, c.submissionsURL = srv.URL+"/tickers", srv.URL+"/subs"
	return c
}

func (r *recorded) note(req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.uas = append(r.uas, req.Header.Get("User-Agent"))
	r.hits = append(r.hits, req.URL.Path)
	r.at = append(r.at, time.Now())
}

func TestClientParsesAndIdentifiesItself(t *testing.T) {
	rec := &recorded{}
	c := newServer(t, rec, 0)
	tk, err := c.Tickers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tk["RY"].CIK != 1067983 || tk["AAPL"].Title != "Apple Inc." {
		t.Errorf("tickers = %+v (keys must be upper-cased)", tk)
	}
	sub, err := c.Submissions(context.Background(), 320193)
	if err != nil {
		t.Fatal(err)
	}
	if sub.Name != "Apple Inc." || sub.BusinessCountry != "CA" || len(sub.Tickers) != 1 {
		t.Errorf("submission = %+v", sub)
	}
	for _, ua := range rec.uas {
		if !strings.Contains(ua, "jo@example.org") {
			t.Errorf("User-Agent %q does not carry the contact", ua)
		}
	}
	if rec.hits[1] != "/subs/CIK0000320193.json" {
		t.Errorf("submissions path = %s, want a ten-digit zero-padded CIK", rec.hits[1])
	}
}

// EDGAR fills addresses.business.country for a foreign filer and leaves
// stateOrCountryDescription null; for a domestic one it is the other way round.
// Reading only one of them left six sensors with no country on the first real run.
func TestClientReadsTheCountryFieldEDGARFilled(t *testing.T) {
	c := newServer(t, &recorded{}, 0)
	foreign, err := c.Submissions(context.Background(), 1000275)
	if err != nil || foreign.BusinessCountry != "Ontario, Canada" {
		t.Errorf("foreign filer = %+v, %v", foreign, err)
	}
	domestic, err := c.Submissions(context.Background(), 320193)
	if err != nil || domestic.BusinessCountry != "CA" {
		t.Errorf("domestic filer = %+v, %v", domestic, err)
	}
	if got := businessCountry("  ", " "); got != "" {
		t.Errorf("both blank = %q", got)
	}
}

func TestClientPacesItsCalls(t *testing.T) {
	rec := &recorded{}
	c := newServer(t, rec, 0)
	c.gap = 40 * time.Millisecond
	for range 3 {
		if _, err := c.Submissions(context.Background(), 320193); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i < len(rec.at); i++ {
		if d := rec.at[i].Sub(rec.at[i-1]); d < 35*time.Millisecond {
			t.Errorf("calls %d and %d were %s apart, want at least the gap", i-1, i, d)
		}
	}
}

func TestClientStatusError(t *testing.T) {
	c := newServer(t, &recorded{}, http.StatusTooManyRequests)
	_, err := c.Tickers(context.Background())
	var se *StatusError
	if !errors.As(err, &se) || !se.Retryable() {
		t.Fatalf("err = %v, want a retryable status error", err)
	}
	if (&StatusError{Code: 403}).Retryable() {
		t.Error("a 403 is not retryable")
	}
}

func TestClientErrorsDoNotCarryTheContact(t *testing.T) {
	c, _ := NewClient("Jo Example jo@example.org")
	c.tickersURL = "http://127.0.0.1:1/tickers"
	_, err := c.Tickers(context.Background())
	if err == nil || strings.Contains(err.Error(), "jo@example.org") {
		t.Errorf("err = %v; it must exist and not echo the contact", err)
	}
}

// --- the refresher ---

type fakeSource struct {
	tickers    map[string]TickerRow
	subs       map[int]Submission
	tickersErr error
	subErr     map[int]error
	calls      int
}

func (f *fakeSource) Tickers(context.Context) (map[string]TickerRow, error) {
	return f.tickers, f.tickersErr
}

func (f *fakeSource) Submissions(_ context.Context, cik int) (Submission, error) {
	f.calls++
	if err := f.subErr[cik]; err != nil {
		return Submission{}, err
	}
	s, ok := f.subs[cik]
	if !ok {
		return Submission{}, &StatusError{Code: 404}
	}
	return s, nil
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

const testCountries = `{"rows":[
 {"sensor":"RY","code":"CA","currency":"CAD","euroArea":false,"edgarCountry":"Ontario, Canada","edgarOverride":false},
 {"sensor":"BAP","code":"PE","currency":"PEN","euroArea":false,"edgarCountry":"Bermuda","edgarOverride":true},
 {"sensor":null,"code":"DE","currency":null,"euroArea":true}]}`

const testSensors = `{"rows":[
 {"symbol":"RY","name":"RBC","group":"country","order":1},
 {"symbol":"BAP","name":"Credicorp","group":"country","order":2},
 {"symbol":"AAPL","name":"Apple","group":"global","theme":"x","order":3}]}`

func newRefresher(src Source) (*Refresher, *memCache) {
	cache := &memCache{data: map[string]string{}}
	fsys := fstest.MapFS{"countries.json": {Data: []byte(testCountries)}, "sensors.json": {Data: []byte(testSensors)}}
	r := NewRefresher(src, reference.New(cache, fsys, quiet()), quiet())
	r.Sleep = func(context.Context, time.Duration) error { return nil }
	r.Now = func() time.Time { return time.Date(2026, 10, 17, 6, 0, 0, 0, time.UTC) }
	return r, cache
}

func goodSource() *fakeSource {
	return &fakeSource{
		tickers: map[string]TickerRow{
			"RY": {CIK: 1000275}, "BAP": {CIK: 1160174}, "AAPL": {CIK: 320193},
		},
		subs: map[int]Submission{
			1000275: {Name: "Royal Bank of Canada", Tickers: []string{"RY"}, BusinessCountry: "Ontario, Canada"},
			1160174: {Name: "Credicorp", Tickers: []string{"BAP"}, BusinessCountry: "Bermuda"},
			320193:  {Name: "Apple Inc.", Tickers: []string{"AAPL"}, BusinessCountry: "CA"},
		},
		subErr: map[int]error{},
	}
}

func kinds(ds Dataset) map[string]string {
	m := map[string]string{}
	for _, d := range ds.Drift {
		m[d.Symbol] = d.Kind
	}
	return m
}

func TestBuildCleanIncludingTheOverrideAndAUSFiler(t *testing.T) {
	r, _ := newRefresher(goodSource())
	ds, err := r.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ds.Drift) != 0 || len(ds.Sensors) != 3 {
		t.Fatalf("drift %v, sensors %v", ds.Drift, ds.Sensors)
	}
	if e := ds.Sensors["RY"]; e.CIK != "0001000275" || e.ISO != "CA" || e.Country != "Ontario, Canada" {
		t.Errorf("RY = %+v", e)
	}
	if e := ds.Sensors["BAP"]; e.ISO != "BM" {
		t.Errorf("BAP = %+v, want its override country Bermuda", e)
	}
	if e := ds.Sensors["AAPL"]; e.ISO != "US" {
		t.Errorf("AAPL = %+v (California is the US, not Canada)", e)
	}
	if ds.GeneratedAt != "2026-10-17T06:00:00Z" || ds.AsOf != "2026-10-17" {
		t.Errorf("stamp = %s %s", ds.GeneratedAt, ds.AsOf)
	}
}

func TestBuildFindsEachKindOfDrift(t *testing.T) {
	src := goodSource()
	delete(src.tickers, "RY")                                                                            // missing from company_tickers.json
	src.subs[1160174] = Submission{Name: "Credicorp", Tickers: []string{"BAP"}, BusinessCountry: "Peru"} // moved from the override country
	src.subs[320193] = Submission{Name: "Apple Inc.", Tickers: []string{"OTHER"}, BusinessCountry: "Atlantis"}
	r, _ := newRefresher(src)
	ds, err := r.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][]string{}
	for _, d := range ds.Drift {
		got[d.Symbol] = append(got[d.Symbol], d.Kind)
	}
	if got["RY"][0] != DriftTickerMissing {
		t.Errorf("RY = %v", got["RY"])
	}
	if got["BAP"][0] != DriftCountry {
		t.Errorf("BAP = %v, want a country mismatch (Peru is not its recorded Bermuda)", got["BAP"])
	}
	if len(got["AAPL"]) != 2 || got["AAPL"][0] != DriftTickerNotFiled || got["AAPL"][1] != DriftCountryUnknown {
		t.Errorf("AAPL = %v", got["AAPL"])
	}
}

func TestBuildSurvivesAFailedFetchAndKeepsTheLastGoodEntry(t *testing.T) {
	r, cache := newRefresher(goodSource())
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.data[reference.CacheKey(DatasetName)]; !ok {
		t.Fatal("dataset was not stored")
	}

	src := goodSource()
	src.subErr[1000275] = &StatusError{Code: 404}
	r2, _ := newRefresher(src)
	r2.Store = r.Store // the stored dataset is the previous run
	ds, err := r2.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if kinds(ds)["RY"] != DriftFetchFailed || ds.Sensors["RY"].CIK != "0001000275" {
		t.Errorf("drift %v, RY entry %+v; the last good entry must be kept", ds.Drift, ds.Sensors["RY"])
	}
}

func TestBuildFailsWhenCompanyTickersCannotBeRead(t *testing.T) {
	src := goodSource()
	src.tickersErr = errors.New("sec down")
	r, _ := newRefresher(src)
	if _, err := r.Build(context.Background()); err == nil {
		t.Error("want an error")
	}
}

func TestACIKChangeIsAdoptedOnlyIfTheNewFilerChecksOut(t *testing.T) {
	prevRun, cache := newRefresher(goodSource())
	if err := prevRun.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}

	// A reorganisation: BAP's ticker now maps to a new CIK whose record is right.
	src := goodSource()
	src.tickers["BAP"] = TickerRow{CIK: 2000001}
	src.subs[2000001] = Submission{Name: "Credicorp Holdings", Tickers: []string{"BAP"}, BusinessCountry: "Bermuda"}
	r, _ := newRefresher(src)
	r.Store = prevRun.Store
	ds, _ := r.Build(context.Background())
	if ds.Sensors["BAP"].CIK != "0002000001" || kinds(ds)["BAP"] != DriftCIKChanged || !strings.Contains(ds.Drift[0].Detail, "adopted") {
		t.Errorf("a sound new CIK must be adopted and reported: %+v %v", ds.Sensors["BAP"], ds.Drift)
	}

	// Same change, but the new filer is somewhere else entirely: keep the old.
	src2 := goodSource()
	src2.tickers["BAP"] = TickerRow{CIK: 2000001}
	src2.subs[2000001] = Submission{Name: "Other Co", Tickers: []string{"BAP"}, BusinessCountry: "Germany"}
	r2, _ := newRefresher(src2)
	r2.Store = prevRun.Store
	ds2, _ := r2.Build(context.Background())
	if ds2.Sensors["BAP"].CIK != "0001160174" {
		t.Errorf("an unsound new CIK was adopted: %+v", ds2.Sensors["BAP"])
	}
	var notAdopted bool
	for _, d := range ds2.Drift {
		notAdopted = notAdopted || (d.Kind == DriftCIKChanged && strings.Contains(d.Detail, "not adopted"))
	}
	if !notAdopted {
		t.Errorf("drift = %v, want a not-adopted CIK change", ds2.Drift)
	}
	_ = cache
}

func TestRefreshStoresThenReportsDrift(t *testing.T) {
	src := goodSource()
	src.subs[1160174] = Submission{Name: "Credicorp", Tickers: []string{"BAP"}, BusinessCountry: "Peru"}
	r, cache := newRefresher(src)
	err := r.Refresh(context.Background())
	var de *DriftError
	if !errors.As(err, &de) || len(de.Findings) != 1 {
		t.Fatalf("err = %v, want a drift error", err)
	}
	if _, ok := cache.data[reference.CacheKey(DatasetName)]; !ok {
		t.Error("the dataset must be stored even when it has drift")
	}
}

func TestRetryRetriesOnlyWhatCanSucceed(t *testing.T) {
	src := goodSource()
	flaky := &flakySource{fakeSource: src, failFirst: 2}
	r, _ := newRefresher(flaky)
	if _, err := r.Build(context.Background()); err != nil {
		t.Fatalf("two 503s then success must work: %v", err)
	}
	src2 := goodSource()
	src2.tickersErr = &StatusError{Code: 403}
	r2, _ := newRefresher(src2)
	if _, err := r2.Build(context.Background()); err == nil {
		t.Error("a 403 must fail without retrying forever")
	}
}

type flakySource struct {
	*fakeSource
	failFirst int
	n         int
}

func (f *flakySource) Tickers(ctx context.Context) (map[string]TickerRow, error) {
	f.n++
	if f.n <= f.failFirst {
		return nil, &StatusError{Code: 503}
	}
	return f.fakeSource.Tickers(ctx)
}

func TestStale(t *testing.T) {
	r, _ := newRefresher(goodSource())
	if !r.Stale(context.Background(), MaxAge) {
		t.Error("with nothing stored the dataset is stale (the embedded copy is a floor)")
	}
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.Stale(context.Background(), MaxAge) {
		t.Error("just refreshed")
	}
	r.Now = func() time.Time { return time.Date(2026, 10, 25, 6, 0, 0, 0, time.UTC) }
	if !r.Stale(context.Background(), MaxAge) {
		t.Error("eight days old must be stale")
	}
}

// The embedded snapshot is the floor the job and step 6 read before the first
// run: eighteen sensors, each with a ten-digit CIK, none with drift.
func TestEmbeddedSnapshot(t *testing.T) {
	var ds Dataset
	if _, err := reference.New(nil, reference.Embedded(), quiet()).Get(context.Background(), DatasetName, &ds); err != nil {
		t.Fatal(err)
	}
	if len(ds.Sensors) != 18 || len(ds.Drift) != 0 || ds.Source == "" || ds.AsOf == "" {
		t.Fatalf("snapshot: %d sensors, %d drift, source %q, asOf %q", len(ds.Sensors), len(ds.Drift), ds.Source, ds.AsOf)
	}
	for sym, e := range ds.Sensors {
		if len(e.CIK) != 10 || e.ISO == "" || e.Name == "" {
			t.Errorf("%s = %+v", sym, e)
		}
	}
	if ds.Sensors["BAP"].ISO != "BM" || ds.Sensors["RY"].ISO != "CA" || ds.Sensors["NVDA"].ISO != "US" {
		t.Error("the override, a foreign filer and a domestic filer must each normalise correctly")
	}
}

// --- filings ---

// The columns are the real submissions shape: parallel arrays, newest first,
// acceptance time in UTC with milliseconds, items a comma-separated string.
const recentJSON = `{"filings":{"recent":{
 "accessionNumber":["0001-26-000005","0001-26-000004","0001-26-000003","0001-26-000002","0001-26-000001"],
 "filingDate":["2026-10-16","2026-10-16","2026-10-15","2026-10-14","2026-10-01"],
 "acceptanceDateTime":["2026-10-17T01:30:46.000Z","2026-10-16T21:10:00.000Z","2026-10-15T12:00:00.000Z","2026-10-14T20:15:00.000Z","2026-10-01T12:00:00.000Z"],
 "form":["424B2","8-K","6-K","8-K/A","8-K"],
 "items":["","2.02,9.01","","5.02","7.01"],
 "primaryDocument":["p.htm","earn.htm","k.htm","a.htm","old.htm"],
 "primaryDocDescription":["FORM 424B2","8-K","FORM 6-K","8-K/A","8-K"]}}}`

func TestParseRecentAndSelect(t *testing.T) {
	all, err := parseRecent(1045810, []byte(recentJSON))
	if err != nil || len(all) != 5 {
		t.Fatalf("%d filings, err %v", len(all), err)
	}
	if all[1].URL != "https://www.sec.gov/Archives/edgar/data/1045810/000126000004/earn.htm" {
		t.Errorf("url = %s", all[1].URL)
	}
	if got := all[1].Items; len(got) != 2 || got[0] != "2.02" || got[1] != "9.01" {
		t.Errorf("items = %v", got)
	}

	from, to := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	got := Select(all, []string{"8-K"}, from, to)
	if len(got) != 2 || got[0].Accession != "0001-26-000002" || got[1].Accession != "0001-26-000004" {
		t.Errorf("8-Ks in the window = %+v, want the amendment and the 16 Oct filing, oldest first (the 424B2 and the 1 Oct 8-K are out)", got)
	}
	if k := Select(all, []string{"6-K"}, from, to); len(k) != 1 || k[0].Description != "FORM 6-K" {
		t.Errorf("6-Ks = %+v", k)
	}
	if len(Select(all, []string{"8-K"}, from.AddDate(0, 0, -30), from.AddDate(0, 0, -20))) != 0 {
		t.Error("an empty window selects nothing")
	}
}

func TestParseRecentRejectsRaggedColumns(t *testing.T) {
	if _, err := parseRecent(1, []byte(`{"filings":{"recent":{"form":["8-K"],"accessionNumber":[]}}}`)); err == nil {
		t.Error("columns of different lengths must be an error, not a misaligned filing")
	}
}

func TestItemTitles(t *testing.T) {
	for code, want := range map[string]string{"2.02": "Results of Operations and Financial Condition", "5.07": "Submission of Matters to a Vote of Security Holders", "9.99": ""} {
		if got := ItemTitle(code); got != want {
			t.Errorf("ItemTitle(%s) = %q, want %q", code, got, want)
		}
	}
	for _, code := range []string{"2.02", "5.02", "5.07"} { // the three the spec names
		if ItemTitle(code) == "" {
			t.Errorf("%s has no title", code)
		}
	}
}
