package ecb

import (
	"archive/zip"
	"bytes"
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

	"github.com/AkashJam/ticker/internal/change"
	"github.com/AkashJam/ticker/internal/fred"
	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/store"
)

func day(y, m, d int, rates map[string]float64) Day {
	return Day{Date: time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC), Rates: rates}
}

const histCSV = `Date,USD,JPY,BGN,CHF,
2026-10-09,1.1206,177.34,N/A,0.9313,
2026-10-08,1.1180,176.00,N/A,0.9300,
`

func histZip(t *testing.T, csv string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("eurofxref-hist.csv")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte(csv))
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

const dailyXML = `<?xml version="1.0" encoding="UTF-8"?>
<gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
	<gesmes:subject>Reference rates</gesmes:subject>
	<Cube>
		<Cube time='2026-10-09'>
			<Cube currency='USD' rate='1.1206'/>
			<Cube currency='JPY' rate='177.34'/>
		</Cube>
	</Cube>
</gesmes:Envelope>`

func TestParseHistory(t *testing.T) {
	days, err := parseHistory(histZip(t, histCSV))
	if err != nil {
		t.Fatal(err)
	}
	if len(days) != 2 || !days[0].Date.Equal(time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("days = %+v, want oldest first", days)
	}
	if days[1].Rates["USD"] != 1.1206 || days[1].Rates["CHF"] != 0.9313 {
		t.Errorf("rates = %v", days[1].Rates)
	}
	if _, ok := days[1].Rates["BGN"]; ok {
		t.Error("an N/A rate was kept")
	}
	if _, err := parseHistory(histZip(t, "Wrong,USD\n")); err == nil {
		t.Error("a bad header was accepted")
	}
	if _, err := parseHistory([]byte("not a zip")); err == nil {
		t.Error("a non-zip was accepted")
	}
}

func TestParseDaily(t *testing.T) {
	d, err := parseDaily([]byte(dailyXML))
	if err != nil || d.Rates["USD"] != 1.1206 || !d.Date.Equal(time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("day = %+v err = %v", d, err)
	}
	if _, err := parseDaily([]byte(`<Envelope><Cube></Cube></Envelope>`)); !errors.Is(err, ErrNoData) {
		t.Errorf("err = %v, want ErrNoData", err)
	}
}

func TestClientStatusAndPaths(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eurofxref-daily.xml":
			_, _ = w.Write([]byte(dailyXML))
		case "/eurofxref-hist.zip":
			_, _ = w.Write(histZip(t, histCSV))
		case "/limited":
			w.Header().Set("Retry-After", "9")
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	c := NewClient()
	c.baseURL = srv.URL

	if d, err := c.Daily(context.Background()); err != nil || d.Rates["JPY"] != 177.34 {
		t.Fatalf("daily = %+v err = %v", d, err)
	}
	if days, err := c.History(context.Background()); err != nil || len(days) != 2 {
		t.Fatalf("history = %d days err = %v", len(days), err)
	}
	var se *StatusError
	if _, err := c.get(context.Background(), "limited"); !errors.As(err, &se) || !se.Retryable() || se.RetryAfter != 9*time.Second {
		t.Errorf("err = %v, want retryable 429 with Retry-After", err)
	}
	if _, err := c.get(context.Background(), "missing"); !errors.As(err, &se) || se.Retryable() {
		t.Errorf("err = %v, want non-retryable 404", err)
	}
}

// weekdays builds one Day per weekday from..to, as the ECB publishes.
func weekdays(from, to time.Time, rates func(time.Time) map[string]float64) []Day {
	var days []Day
	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		if wd := d.Weekday(); wd == time.Saturday || wd == time.Sunday {
			continue
		}
		days = append(days, Day{Date: d, Rates: rates(d)})
	}
	return days
}

func ymd(y, m, d int) time.Time { return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC) }

func TestCarryForwardAcrossWeekendAndAnnualAverage(t *testing.T) {
	st := NewState([]string{"USD"})
	// Monday 5 Jan at 1.0, then Thursday 8 Jan at 1.3: 6 and 7 Jan carry 1.0.
	st.Append([]Day{day(2026, 1, 5, map[string]float64{"USD": 1.0}), day(2026, 1, 8, map[string]float64{"USD": 1.3})})
	got := st.Window["USD"].Rates
	want := []float64{1.0, 1.0, 1.0, 1.3}
	if len(got) != 4 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("window = %v, want %v", got, want)
	}
	// The ECB's own definition: the mean of the two days it published, not of the
	// four calendar days. The carried-forward weekend does not count.
	if avg := st.annualAverages("USD")["2026"]; avg != 1.15 {
		t.Errorf("2026 average = %v, want 1.15", avg)
	}
}

func TestMonthlySeriesIsCompleteMonthsOnly(t *testing.T) {
	// 100 in January, 200 in February, 300 in March; the state stops on 15 March.
	rate := func(d time.Time) map[string]float64 { return map[string]float64{"JPY": 100 * float64(d.Month())} }
	st := NewState([]string{"JPY"})
	st.Append(weekdays(ymd(2010, 1, 1), ymd(2010, 3, 15), rate))

	s, ok := st.monthly("JPY")
	if !ok || s.Start != "2010-01" || s.Step != 1 || len(s.Values) != 2 || s.Values[0] != 100 || s.Values[1] != 200 {
		t.Fatalf("series = %+v ok = %v, want January and February only", s, ok)
	}
	if got := st.Compute(time.Now()).Currencies["JPY"].AsOf; got != "2010-02" {
		t.Errorf("asOf = %s, want 2010-02 (March is not complete)", got)
	}
}

func TestMonthlyAverageIsOverPublishedDays(t *testing.T) {
	// Friday 1 Jan at 10, Monday 4 Jan at 20: the 2nd and 3rd are carried forward
	// for the daily window but are not publications, so January is (10+20)/2.
	st := NewState([]string{"USD"})
	st.Append([]Day{day(2010, 1, 1, map[string]float64{"USD": 10}), day(2010, 1, 4, map[string]float64{"USD": 20})})
	if m := st.Sums["USD"]["2010-01"]; m.Days != 2 || m.Sum != 30 {
		t.Errorf("January = %+v, want 2 published days summing 30", m)
	}
	if got := st.Window["USD"].Rates; len(got) != 4 {
		t.Errorf("daily window = %v, want 4 calendar days", got)
	}
}

func TestStrongerCurrencyIsPositiveOnceInverted(t *testing.T) {
	// The yen goes from 100 to 80 per euro through 2010: it bought more euro.
	rate := func(d time.Time) map[string]float64 {
		if d.Month() == time.December {
			return map[string]float64{"JPY": 80}
		}
		return map[string]float64{"JPY": 100}
	}
	st := NewState([]string{"JPY"})
	st.Append(weekdays(ymd(2010, 1, 1), ymd(2010, 12, 31), rate))
	s, ok := st.monthly("JPY")
	if !ok {
		t.Fatal("no series")
	}
	if w := s.Invert().Window(2010, 2010); w == nil || *w != 25 {
		t.Errorf("inverted window = %v, want +25", w)
	}
}

func TestUnpublishedCurrencyIsNullWithReason(t *testing.T) {
	st := NewState([]string{"CLP", "USD"})
	st.Append([]Day{day(2026, 1, 5, map[string]float64{"USD": 1.0})})
	c := st.Compute(time.Now()).Currencies["CLP"]
	if c.Series != nil || c.Reason == "" || c.Source == "" || c.AsOf == "" {
		t.Errorf("CLP = %+v, want no series, a reason, a source and an as-of", c)
	}
}

func monthlySeries(v ...float64) change.Series {
	return change.Series{Start: "2010-01", Step: 1, Values: v}
}

func TestViaUSD(t *testing.T) {
	// 900 pesos a dollar at 1.2 dollars a euro is 1080 pesos a euro.
	got, reason := ViaUSD(monthlySeries(900, 1000), monthlySeries(1.2, 1.1))
	if reason != "" || len(got.Values) != 2 || got.Values[0] != 1080 || got.Values[1] != 1100 {
		t.Errorf("via USD = %+v reason %q", got, reason)
	}

	// Pesos one month short of the ECB's: served to the shorter end.
	usd := monthlySeries(1, 1, 1, 1, 1)
	got, reason = ViaUSD(monthlySeries(10, 10, 10, 10), usd)
	if reason != "" || len(got.Values) != 4 || got.AsOf() != "2010-04" {
		t.Errorf("one month behind: %+v reason %q", got, reason)
	}

	// Three months behind is the limit; four is stale.
	if _, reason := ViaUSD(monthlySeries(10, 10), usd); reason != "" {
		t.Errorf("three months behind: reason %q, want served", reason)
	}
	if _, reason := ViaUSD(monthlySeries(10), usd); reason != "stale" {
		t.Errorf("four months behind: reason %q, want stale", reason)
	}
}

func TestFirstFullMonthStartsTheSeries(t *testing.T) {
	usd := func(time.Time) map[string]float64 { return map[string]float64{"USD": 1} }
	// ILS is first published on Monday 3 January 2011, the ECB's first business
	// day of the year (1 and 2 January are a weekend): January is a full month.
	st := NewState([]string{"ILS", "USD"})
	days := weekdays(ymd(2010, 12, 1), ymd(2011, 4, 15), usd)
	for i := range days {
		if !days[i].Date.Before(ymd(2011, 1, 3)) {
			days[i].Rates["ILS"] = 5
		}
	}
	st.Append(days)
	s, ok := st.monthly("ILS")
	if !ok || s.Start != "2011-01" || len(s.Values) != 3 || s.AsOf() != "2011-03" {
		t.Fatalf("series = %+v ok = %v, want January to March 2011", s, ok)
	}
	if s.Window(2010, 2011) != nil {
		t.Error("a window needing 2010 was served for a currency with no 2010 rate")
	}

	// Published only from the 17th: January is partial, so the series starts in February.
	st = NewState([]string{"ILS", "USD"})
	days = weekdays(ymd(2010, 12, 1), ymd(2011, 4, 15), func(time.Time) map[string]float64 { return map[string]float64{"USD": 1} })
	for i := range days {
		if !days[i].Date.Before(ymd(2011, 1, 17)) {
			days[i].Rates["ILS"] = 5
		}
	}
	st.Append(days)
	if s, ok := st.monthly("ILS"); !ok || s.Start != "2011-02" {
		t.Errorf("series = %+v ok = %v, want it to start in February 2011", s, ok)
	}
}

func TestViaUSDAlignsByMonth(t *testing.T) {
	// The ECB series starts a month later than the OECD one: the product starts
	// where both exist, not where they happen to share an index.
	pesos := monthlySeries(900, 910, 920)
	usd := change.Series{Start: "2010-02", Step: 1, Values: []float64{2, 3}}
	got, reason := ViaUSD(pesos, usd)
	if reason != "no overlap" || len(got.Values) != 0 {
		t.Errorf("got %+v reason %q: January has no dollar rate, so nothing may be derived", got, reason)
	}
}

func TestWindowIsTrimmed(t *testing.T) {
	st := NewState([]string{"USD"})
	var days []Day
	start := ymd(2025, 1, 1)
	for i := 0; i < 450; i++ {
		days = append(days, Day{Date: start.AddDate(0, 0, i), Rates: map[string]float64{"USD": float64(i + 1)}})
	}
	st.Append(days)
	w := st.Window["USD"]
	if len(w.Rates) != windowDays || !w.Start.Equal(start.AddDate(0, 0, 50)) {
		t.Fatalf("len=%d start=%v, want %d days starting 50 days in", len(w.Rates), w.Start, windowDays)
	}
	// The monthly sums still count every day, trimmed or not.
	if m := st.Sums["USD"]["2025-01"]; m.Days != 31 {
		t.Errorf("January 2025 days = %d, want 31", m.Days)
	}
	if avg := st.annualAverages("USD")["2025"]; avg == 0 {
		t.Error("no 2025 average")
	}
}

// --- refresher ---

type fakeFeed struct {
	history  []Day
	daily    Day
	histErr  error
	dailyErr error
	histN    int
	dailyN   int
}

func (f *fakeFeed) History(context.Context) ([]Day, error) {
	f.histN++
	return f.history, f.histErr
}

func (f *fakeFeed) Daily(context.Context) (Day, error) {
	f.dailyN++
	return f.daily, f.dailyErr
}

type fakePesos struct {
	obs []fred.Obs
	err error
	n   int
}

func (f *fakePesos) Series(context.Context, string) ([]fred.Obs, error) {
	f.n++
	return f.obs, f.err
}

// pesosTo builds monthly pesos-per-dollar observations from January 2010
// through the given month of 2026.
func pesosTo(month int) []fred.Obs {
	var o []fred.Obs
	for d := ymd(2010, 1, 1); !d.After(ymd(2026, month, 1)); d = d.AddDate(0, 1, 0) {
		o = append(o, fred.Obs{Date: d, Value: 900})
	}
	return o
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

const countriesJSON = `{"rows":[{"currency":null},{"currency":"USD"},{"currency":"CLP"},{"currency":"USD"}]}`

func testRefresher(feed Feed, pesos Pesos) (*Refresher, *memCache) {
	cache := &memCache{data: map[string]string{}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fsys := fstest.MapFS{"countries.json": {Data: []byte(countriesJSON)}}
	r := NewRefresher(feed, pesos, reference.New(cache, fsys, log), log)
	r.Sleep = func(context.Context, time.Duration) error { return nil }
	r.Now = func() time.Time { return time.Date(2026, 10, 10, 6, 0, 0, 0, time.UTC) }
	return r, cache
}

func key(name string) string { return reference.CacheKey(name) }

// usdHistory is USD at 1.10 on every weekday from 2010 to 8 October 2026.
func usdHistory() []Day {
	return weekdays(ymd(2010, 1, 1), ymd(2026, 10, 8), func(time.Time) map[string]float64 { return map[string]float64{"USD": 1.10} })
}

func TestRefreshSeedsThenAppends(t *testing.T) {
	feed := &fakeFeed{history: usdHistory(), daily: day(2026, 10, 9, map[string]float64{"USD": 1.12})}
	r, cache := testRefresher(feed, &fakePesos{obs: pesosTo(8)})
	ctx := context.Background()

	if err := r.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if feed.histN != 1 || feed.dailyN != 0 {
		t.Fatalf("first run: history=%d daily=%d, want a seed and no daily", feed.histN, feed.dailyN)
	}
	for _, k := range []string{"ecb", "ecb_state"} {
		if _, ok := cache.data[key(k)]; !ok {
			t.Errorf("%s was not stored", k)
		}
	}

	if err := r.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if feed.histN != 1 || feed.dailyN != 1 {
		t.Fatalf("second run: history=%d daily=%d, want the daily file only", feed.histN, feed.dailyN)
	}
	var ds Dataset
	if _, err := r.Store.Get(ctx, DatasetName, &ds); err != nil {
		t.Fatal(err)
	}
	usd := ds.Currencies["USD"]
	if ds.AsOf != "2026-10-09" || usd.Series == nil || usd.AsOf != "2026-09" {
		t.Errorf("asOf %s, USD asOf %q: want the latest day and the last complete month", ds.AsOf, usd.AsOf)
	}
	if _, ok := ds.Currencies["EUR"]; ok || len(ds.Currencies) != 2 {
		t.Errorf("currencies = %v, want USD and CLP only", ds.Currencies)
	}
	if usd.AnnualAverage["2026"] == 0 || usd.AnnualAverage["2015"] != 1.1 {
		t.Errorf("annual averages = %v", usd.AnnualAverage)
	}
}

func TestRefreshDerivesChileViaTheDollar(t *testing.T) {
	r, _ := testRefresher(&fakeFeed{history: usdHistory()}, &fakePesos{obs: pesosTo(8)})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	var ds Dataset
	if _, err := r.Store.Get(context.Background(), DatasetName, &ds); err != nil {
		t.Fatal(err)
	}
	clp := ds.Currencies["CLP"]
	if clp.Series == nil || clp.Derived != "via USD" || clp.Reason != "" || clp.AsOf != "2026-08" {
		t.Fatalf("CLP = %+v, want derived, with the OECD series' last month", clp)
	}
	if clp.Series.Values[0] != 990 { // 900 pesos a dollar × 1.10 dollars a euro
		t.Errorf("first value = %v, want 990", clp.Series.Values[0])
	}
}

func TestChileFallsBackToAStatedGapWhenTheOECDSeriesIsStale(t *testing.T) {
	// The ECB's last complete month is September; the OECD stops in May.
	r, _ := testRefresher(&fakeFeed{history: usdHistory()}, &fakePesos{obs: pesosTo(5)})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("a stale series is a normal state, not an error: %v", err)
	}
	var ds Dataset
	if _, err := r.Store.Get(context.Background(), DatasetName, &ds); err != nil {
		t.Fatal(err)
	}
	if clp := ds.Currencies["CLP"]; clp.Series != nil || clp.Reason != "stale" || clp.Derived != "via USD" || clp.AsOf == "" {
		t.Errorf("CLP = %+v, want null, stale, derived, with an as-of", clp)
	}
	if ds.Currencies["USD"].Series == nil {
		t.Error("USD was lost with Chile")
	}
}

func TestRefreshReseedsOnLongGap(t *testing.T) {
	feed := &fakeFeed{history: usdHistory(), daily: day(2026, 10, 9, map[string]float64{"USD": 1.12})}
	r, _ := testRefresher(feed, &fakePesos{obs: pesosTo(8)})
	ctx := context.Background()
	if err := r.Refresh(ctx); err != nil { // seeds at 8 October
		t.Fatal(err)
	}
	feed.daily = day(2026, 11, 20, map[string]float64{"USD": 1.12})
	feed.history = append(usdHistory(), day(2026, 11, 20, map[string]float64{"USD": 1.12}))
	if err := r.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if feed.histN != 2 {
		t.Errorf("history fetched %d times, want a re-seed after a 43-day gap", feed.histN)
	}
}

func TestRefreshIsAllOrNothing(t *testing.T) {
	feed := &fakeFeed{history: usdHistory(), daily: day(2026, 10, 9, nil)}
	pesos := &fakePesos{obs: pesosTo(8)}
	r, cache := testRefresher(feed, pesos)
	ctx := context.Background()
	if err := r.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	before := map[string]string{"ecb": cache.data[key("ecb")], "state": cache.data[key("ecb_state")]}

	feed.dailyErr = &StatusError{Code: 400}
	if err := r.Refresh(ctx); err == nil {
		t.Fatal("expected an error")
	}
	if feed.dailyN != 1 {
		t.Errorf("a non-retryable 400 was tried %d times", feed.dailyN)
	}
	if cache.data[key("ecb")] != before["ecb"] || cache.data[key("ecb_state")] != before["state"] {
		t.Error("a failed refresh changed the stored blobs")
	}
}

func TestAFailedPesosFetchWritesNothing(t *testing.T) {
	pesos := &fakePesos{err: &StatusError{Code: 503}}
	r, cache := testRefresher(&fakeFeed{history: usdHistory()}, pesos)
	if err := r.Refresh(context.Background()); err == nil {
		t.Fatal("expected an error")
	}
	if pesos.n != r.Attempts {
		t.Errorf("pesos fetched %d times, want %d", pesos.n, r.Attempts)
	}
	if len(cache.data) != 0 {
		t.Errorf("a failed pesos fetch wrote %d blobs", len(cache.data))
	}
}

func TestRefreshRetriesTransientErrors(t *testing.T) {
	feed := &fakeFeed{histErr: &StatusError{Code: 503}}
	r, _ := testRefresher(feed, &fakePesos{})
	if err := r.Refresh(context.Background()); err == nil || feed.histN != r.Attempts {
		t.Errorf("err = %v after %d attempts, want %d", err, feed.histN, r.Attempts)
	}
}

func TestRefreshReseedsWhenCurrencySetChanges(t *testing.T) {
	feed := &fakeFeed{history: usdHistory(), daily: day(2026, 10, 9, map[string]float64{"USD": 1.1})}
	r, cache := testRefresher(feed, &fakePesos{obs: pesosTo(8)})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.Store = reference.New(cache, fstest.MapFS{"countries.json": {Data: []byte(`{"rows":[{"currency":"USD"},{"currency":"JPY"}]}`)}}, nil)
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if feed.histN != 2 {
		t.Errorf("history fetched %d times, want a re-seed for the new currency", feed.histN)
	}
}

func TestStale(t *testing.T) {
	r, _ := testRefresher(&fakeFeed{history: usdHistory()}, &fakePesos{obs: pesosTo(8)})
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

func TestErrorsNameTheFile(t *testing.T) {
	_, err := parseHistory(histZip(t, "Date,USD\nnot-a-date,1\n"))
	if err == nil || !strings.Contains(err.Error(), "not-a-date") {
		t.Errorf("err = %v", err)
	}
}
