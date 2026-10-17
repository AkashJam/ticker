package api

import (
	"net/http"
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/trigger"
)

type recordBody struct {
	Symbol  string `json:"symbol"`
	Name    string `json:"name"`
	Country *struct {
		Slug string `json:"slug"`
		Name string `json:"name"`
	} `json:"country"`
	Theme           *string `json:"theme"`
	Benchmark       *string `json:"benchmark"`
	BenchmarkReason *string `json:"benchmarkReason"`
	BaseSession     *string `json:"baseSession"`
	LatestSession   *string `json:"latestSession"`
	EventCount      int     `json:"eventCount"`
	Sessions        int     `json:"sessionsObserved"`
	JudgedFrom      string  `json:"judgedFrom"`
	Judged          int     `json:"sessionsJudged"`
	Series          struct {
		Dates          []string  `json:"dates"`
		Close          []float64 `json:"close"`
		BenchmarkClose []float64 `json:"benchmarkClose"`
	} `json:"series"`
	Events []struct {
		ID          string `json:"id"`
		SessionDate string `json:"sessionDate"`
		SessionsAgo int    `json:"sessionsAgo"`
		Price       *struct {
			ChangePercent float64 `json:"changePercent"`
		} `json:"price"`
		Band *struct{} `json:"band"`
	} `json:"events"`
}

func fetchRecord(t *testing.T, h *Handlers, name string) (recordBody, int) {
	t.Helper()
	var body recordBody
	code := get(t, ledgerRouter(h), "/market/sensors/"+name, &body)
	return body, code
}

func TestRecordResolvesSymbolAndSlugToOneBody(t *testing.T) {
	h := newHandlers(t, nil, fixtureBars())
	a, codeA := fetchRecord(t, h, "AMX")
	b, codeB := fetchRecord(t, h, "mexico")
	c, codeC := fetchRecord(t, h, "amx")
	if codeA != http.StatusOK || codeB != http.StatusOK || codeC != http.StatusOK {
		t.Fatalf("codes %d %d %d", codeA, codeB, codeC)
	}
	if a.Symbol != "AMX" || b.Symbol != "AMX" || c.Symbol != "AMX" {
		t.Errorf("canonical symbols %q %q %q", a.Symbol, b.Symbol, c.Symbol)
	}
	if a.Country == nil || a.Country.Slug != "mexico" || a.Theme != nil {
		t.Errorf("country %+v theme %v", a.Country, a.Theme)
	}
	if a.Benchmark == nil || *a.Benchmark != "IXP" {
		t.Errorf("benchmark %v", a.Benchmark)
	}
}

func TestRecordIs404ForAnythingButASensor(t *testing.T) {
	h := newHandlers(t, nil, fixtureBars())
	for _, name := range []string{"SIM:NOVA", "IXP", "nowhere", "france", "SPY"} {
		if _, code := fetchRecord(t, h, name); code != http.StatusNotFound {
			t.Errorf("%s = %d, want 404", name, code)
		}
	}
}

func TestRecordSeriesIsAlignedFromTheFirstSharedBar(t *testing.T) {
	b := fixtureBars()
	b["IXP"] = b["IXP"][3:] // the benchmark starts three sessions later
	h := newHandlers(t, nil, b)
	body, _ := fetchRecord(t, h, "AMX")
	want := day0.AddDate(0, 0, 3).Format(time.DateOnly)
	if body.BaseSession == nil || *body.BaseSession != want {
		t.Fatalf("baseSession %v, want %s", body.BaseSession, want)
	}
	n := len(body.Series.Dates)
	if n != 67 || body.Sessions != 67 || len(body.Series.Close) != n || len(body.Series.BenchmarkClose) != n {
		t.Fatalf("series lengths %d %d %d, sessions %d", n, len(body.Series.Close), len(body.Series.BenchmarkClose), body.Sessions)
	}
	if body.LatestSession == nil || *body.LatestSession != session.Format(time.DateOnly) {
		t.Errorf("latestSession %v", body.LatestSession)
	}
	// The fixture's 70 bars end on 8 Sep, before the trigger's go-live: nothing judged.
	if body.JudgedFrom != "2026-10-16" || body.Judged != 0 {
		t.Errorf("judgedFrom %q judged %d", body.JudgedFrom, body.Judged)
	}
}

func TestRecordForASensorWithNoBenchmark(t *testing.T) {
	b := fixtureBars()
	b["AAPL"] = bars("AAPL", 70, alt)
	h := newHandlers(t, nil, b)
	body, code := fetchRecord(t, h, "AAPL")
	if code != http.StatusOK || body.Benchmark != nil || body.BenchmarkReason == nil || body.Series.BenchmarkClose != nil {
		t.Fatalf("code %d benchmark %v reason %v series %v", code, body.Benchmark, body.BenchmarkReason, body.Series.BenchmarkClose)
	}
	if len(body.Series.Close) != 70 || body.Theme == nil {
		t.Errorf("closes %d theme %v", len(body.Series.Close), body.Theme)
	}
}

func TestRecordCarriesEveryEventNewestFirstWithoutBand(t *testing.T) {
	older := tripFor("AMX", "IXP", session.AddDate(0, 0, -5))
	newer := tripFor("AMX", "IXP", session)
	other := tripFor("CIB", "", session)
	h := newHandlers(t, []trigger.Trip{newer, other, older}, fixtureBars())
	body, _ := fetchRecord(t, h, "AMX")
	if body.EventCount != 2 || len(body.Events) != 2 {
		t.Fatalf("events %d count %d", len(body.Events), body.EventCount)
	}
	if body.Events[0].SessionDate != session.Format(time.DateOnly) || body.Events[0].ID != "mexico-"+session.Format(time.DateOnly) {
		t.Errorf("first event %+v", body.Events[0])
	}
	if body.Events[0].Band != nil || body.Events[1].Band != nil {
		t.Error("a record event carries a band")
	}
	if body.Events[1].SessionsAgo != 5 {
		t.Errorf("sessionsAgo %d, want 5", body.Events[1].SessionsAgo)
	}
	if body.Events[0].Price == nil {
		t.Error("event has no price move")
	}
}

func TestRecordWithNoEventsIsAnEmptyList(t *testing.T) {
	h := newHandlers(t, nil, fixtureBars())
	body, code := fetchRecord(t, h, "KB")
	if code != http.StatusOK || body.Events == nil || len(body.Events) != 0 || body.EventCount != 0 {
		t.Fatalf("code %d events %v", code, body.Events)
	}
}
