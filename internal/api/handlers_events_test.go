package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/trigger"
)

var (
	day0    = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	session = day0.AddDate(0, 0, 69) // the tested session: 70 consecutive daily bars end on it
)

// fakeBars serves daily bars for the symbols it was given, honouring [from, to).
type fakeBars map[string][]source.Candle

func (f fakeBars) Candles(_ context.Context, symbol, _ string, from, to time.Time) ([]source.Candle, error) {
	var out []source.Candle
	for _, b := range f[symbol] {
		if !from.IsZero() && b.Time.Before(from) {
			continue
		}
		if !to.IsZero() && !b.Time.Before(to) {
			continue
		}
		out = append(out, b)
	}
	return out, nil
}

type fakeEventStore struct {
	trips []trigger.Trip
	err   error
}

func (f fakeEventStore) Events(_ context.Context, limit int) ([]trigger.Trip, error) {
	if f.err != nil {
		return nil, f.err
	}
	if len(f.trips) > limit {
		return f.trips[:limit], nil
	}
	return f.trips, nil
}

func (f fakeEventStore) EventsFor(_ context.Context, symbol string) ([]trigger.Trip, error) {
	if f.err != nil {
		return nil, f.err
	}
	var out []trigger.Trip
	for _, t := range f.trips {
		if t.Symbol == symbol {
			out = append(out, t)
		}
	}
	return out, nil
}

// bars builds n consecutive daily bars from day0 whose close moves by
// step(i) from 100; the sensors alternate +-1% so sigma is positive.
func bars(symbol string, n int, step func(i int) float64) []source.Candle {
	out := make([]source.Candle, n)
	px := 100.0
	for i := range n {
		if i > 0 {
			px *= 1 + step(i)
		}
		out[i] = source.Candle{Time: day0.AddDate(0, 0, i), Symbol: symbol, Interval: "1d", Close: px}
	}
	return out
}

func alt(i int) float64 {
	if i%2 == 0 {
		return 0.01
	}
	return -0.01
}

func flat(int) float64 { return 0 }

func newHandlers(t *testing.T, trips []trigger.Trip, b fakeBars) *Handlers {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &Handlers{
		Events: fakeEventStore{trips: trips}, Candles: b, Log: log,
		Ref: reference.New(nil, reference.Embedded(), log),
	}
}

func ledgerRouter(h *Handlers) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/market/events", h.GetEvents)
	r.GET("/market/tape", h.GetTape)
	r.GET("/market/sensors/:symbol", h.GetSensorRecord)
	r.GET("/market/state", h.GetMarketState)
	return r
}

func str(s string) *string { return &s }

func tripFor(symbol, bench string, d time.Time) trigger.Trip {
	t := trigger.Trip{
		Symbol: symbol, SessionDate: d, ClosedAt: d.Add(20 * time.Hour),
		SymbolReturn: 0.05, Residual: 0.05, Sigma: 0.01, Z: 5, Threshold: 2.5, WindowSessions: 60,
		Caveats: []string{trigger.CaveatSelfWeight},
	}
	if bench != "" {
		t.Benchmark = str(bench)
		b := 0.0
		t.BenchmarkReturn = &b
	} else {
		t.BenchmarkReason = str("no benchmark")
	}
	return t
}

func fixtureBars() fakeBars {
	n := 70
	jump := func(i int) float64 {
		if i == n-1 {
			return 0.05
		}
		return alt(i)
	}
	return fakeBars{
		"RY":  bars("RY", n, flat), // the canary
		"AMX": bars("AMX", n, jump),
		"IXP": bars("IXP", n, flat),
		"CAT": bars("CAT", n, jump),
		"CIB": bars("CIB", n, jump),
		"KB":  bars("KB", n, jump),
	}
}

type eventsBody struct {
	Events []struct {
		ID          string `json:"id"`
		Symbol      string `json:"symbol"`
		Name        string `json:"name"`
		SessionDate string `json:"sessionDate"`
		SessionsAgo int    `json:"sessionsAgo"`
		Country     *struct {
			Slug  string `json:"slug"`
			Name  string `json:"name"`
			Atlas bool   `json:"atlas"`
		} `json:"country"`
		Theme *string `json:"theme"`
		Price *struct {
			Close         float64 `json:"close"`
			PrevClose     float64 `json:"prevClose"`
			ChangePercent float64 `json:"changePercent"`
		} `json:"price"`
		Trigger struct {
			Z         float64 `json:"z"`
			Threshold float64 `json:"threshold"`
		} `json:"trigger"`
		Band *struct {
			Dates []string  `json:"dates"`
			Close []float64 `json:"close"`
			Lower []float64 `json:"lower"`
			Upper []float64 `json:"upper"`
		} `json:"band"`
		Split    json.RawMessage `json:"split"`
		Coverage json.RawMessage `json:"coverage"`
		Caveats  []string        `json:"caveats"`
		Thin     *struct {
			UsdM float64 `json:"usdM"`
			AsOf string  `json:"asOf"`
		} `json:"thin"`
	} `json:"events"`
	Truncated     bool    `json:"truncated"`
	LatestSession *string `json:"latestSession"`
}

func fetchEvents(t *testing.T, h *Handlers) eventsBody {
	t.Helper()
	var body eventsBody
	if code := get(t, ledgerRouter(h), "/market/events", &body); code != http.StatusOK {
		t.Fatalf("GET /market/events = %d", code)
	}
	return body
}

func TestEventsCarryEverythingTheLedgerNeeds(t *testing.T) {
	amx := tripFor("AMX", "IXP", session)
	amx.Split = []byte(`{"status":"ok","label":"both"}`)
	amx.SplitLabel = str("both")
	amx.Coverage = []byte(`{"stage":"trip","complete":false}`)
	cat := tripFor("CAT", "", session)
	h := newHandlers(t, []trigger.Trip{amx, cat}, fixtureBars())
	body := fetchEvents(t, h)

	if len(body.Events) != 2 || body.Truncated || body.LatestSession == nil || *body.LatestSession != "2026-09-08" {
		t.Fatalf("events %d truncated %v latest %v", len(body.Events), body.Truncated, body.LatestSession)
	}
	a, c := body.Events[0], body.Events[1]
	if a.ID != "mexico-2026-09-08" || a.Country == nil || a.Country.Name != "Mexico" || !a.Country.Atlas || a.Theme != nil {
		t.Errorf("AMX = %+v", a)
	}
	if c.ID != "cat-2026-09-08" || c.Country != nil || c.Theme == nil || *c.Theme != "Industrial cycle" {
		t.Errorf("CAT = id %s country %v theme %v: a global name has no country, and a theme", c.ID, c.Country, c.Theme)
	}
	if a.Name == "" || a.SessionsAgo != 0 {
		t.Errorf("name %q, sessionsAgo %d", a.Name, a.SessionsAgo)
	}
	if a.Price == nil || math.Abs(a.Price.ChangePercent-5) > 0.5 || a.Price.Close <= a.Price.PrevClose {
		t.Errorf("price = %+v, want about +5%%", a.Price)
	}
	if string(a.Split) != `{"status":"ok","label":"both"}` || string(a.Coverage) != `{"stage":"trip","complete":false}` {
		t.Errorf("split %s coverage %s: stored JSON passes through whole", a.Split, a.Coverage)
	}
	if string(c.Split) != "null" || string(c.Coverage) != "null" {
		t.Errorf("a pending split and no coverage are null, got %s %s", c.Split, c.Coverage)
	}
	if len(a.Caveats) != 1 || a.Caveats[0] != trigger.CaveatSelfWeight {
		t.Errorf("caveats = %v", a.Caveats)
	}
}

func TestEventBandIsTheSectorAdjustedEdgesAroundTheTripsSigma(t *testing.T) {
	h := newHandlers(t, []trigger.Trip{tripFor("AMX", "IXP", session)}, fixtureBars())
	band := fetchEvents(t, h).Events[0].Band
	if band == nil || len(band.Dates) != trigger.BandWindow {
		t.Fatalf("band = %+v, want %d sessions", band, trigger.BandWindow)
	}
	n := len(band.Dates)
	if band.Dates[n-1] != "2026-09-08T00:00:00Z" {
		t.Errorf("the band ends on %s, the tripped session", band.Dates[n-1])
	}
	// The benchmark is flat, so the edges are the previous close x (1 +- 2.5 x 0.01).
	prevClose := band.Close[n-2]
	if math.Abs(band.Lower[n-1]-prevClose*0.975) > 1e-9 || math.Abs(band.Upper[n-1]-prevClose*1.025) > 1e-9 {
		t.Errorf("edges %v %v, want %v and %v", band.Lower[n-1], band.Upper[n-1], prevClose*0.975, prevClose*1.025)
	}
	// The 5% jump is outside the band, an ordinary +-1% day inside it.
	if band.Close[n-1] <= band.Upper[n-1] {
		t.Error("the tripped close must be above the band")
	}
	if band.Close[n-5] < band.Lower[n-5] || band.Close[n-5] > band.Upper[n-5] {
		t.Error("an ordinary day must sit inside the band")
	}
}

func TestEventCountryForColombiaIsNamedButHasNoAtlasRow(t *testing.T) {
	h := newHandlers(t, []trigger.Trip{tripFor("CIB", "IXG", session)}, fixtureBars())
	ev := fetchEvents(t, h).Events[0]
	if ev.Country == nil || ev.Country.Name != "Colombia" || ev.Country.Atlas || ev.ID != "colombia-2026-09-08" {
		t.Errorf("CIB = %+v: the heading needs the name, and the view links to /atlas only when atlas is true", ev.Country)
	}
}

func TestEventThinListingCarriesTheMeasuredFigure(t *testing.T) {
	h := newHandlers(t, []trigger.Trip{tripFor("KB", "IXG", session)}, fixtureBars())
	th := fetchEvents(t, h).Events[0].Thin
	if th == nil || th.UsdM <= 0 || th.AsOf == "" {
		t.Errorf("KB thin = %+v", th)
	}
	h = newHandlers(t, []trigger.Trip{tripFor("AMX", "IXP", session)}, fixtureBars())
	if fetchEvents(t, h).Events[0].Thin != nil {
		t.Error("AMX is not thin")
	}
}

func TestEventSessionsAgoCountsTheCanarysLaterBars(t *testing.T) {
	older := tripFor("AMX", "IXP", session.AddDate(0, 0, -3))
	h := newHandlers(t, []trigger.Trip{tripFor("CAT", "", session), older}, fixtureBars())
	body := fetchEvents(t, h)
	if body.Events[0].SessionsAgo != 0 || body.Events[1].SessionsAgo != 3 {
		t.Errorf("sessionsAgo = %d, %d, want 0 and 3", body.Events[0].SessionsAgo, body.Events[1].SessionsAgo)
	}
}

func TestEventsEmptyTruncatedAndFailing(t *testing.T) {
	body := fetchEvents(t, newHandlers(t, nil, fixtureBars()))
	if body.Events == nil || len(body.Events) != 0 || body.Truncated {
		t.Errorf("an empty ledger is an empty list: %+v", body)
	}

	many := make([]trigger.Trip, eventsLimit+1)
	for i := range many {
		many[i] = tripFor("AMX", "IXP", session.AddDate(0, 0, -i%10))
	}
	h := newHandlers(t, many, fixtureBars())
	if body := fetchEvents(t, h); !body.Truncated || len(body.Events) != eventsLimit {
		t.Errorf("events %d truncated %v; past the cap the response says so", len(body.Events), body.Truncated)
	}

	h = newHandlers(t, nil, fixtureBars())
	h.Events = fakeEventStore{err: context.DeadlineExceeded}
	if code := get(t, ledgerRouter(h), "/market/events", nil); code != http.StatusInternalServerError {
		t.Errorf("a store failure = %d, want 500", code)
	}
}

func TestEventWithNoBarsStillAppearsWithoutPriceOrBand(t *testing.T) {
	b := fixtureBars()
	delete(b, "AMX")
	ev := fetchEvents(t, newHandlers(t, []trigger.Trip{tripFor("AMX", "IXP", session)}, b)).Events[0]
	if ev.Price != nil || ev.Band != nil || ev.Trigger.Z != 5 {
		t.Errorf("the recorded figures stand even when the bars are gone: %+v", ev)
	}
}

// --- tripped ---

func state(t *testing.T, h *Handlers) bool {
	t.Helper()
	var got struct {
		Tripped bool `json:"tripped"`
	}
	if code := get(t, ledgerRouter(h), "/market/state", &got); code != http.StatusOK {
		t.Fatalf("state = %d", code)
	}
	return got.Tripped
}

func TestTrippedForExactlyOneSession(t *testing.T) {
	b := fixtureBars()
	if !state(t, newHandlers(t, []trigger.Trip{tripFor("CAT", "", session)}, b)) {
		t.Error("the newest event is from the newest session: tripped")
	}
	if state(t, newHandlers(t, []trigger.Trip{tripFor("CAT", "", session.AddDate(0, 0, -1))}, b)) {
		t.Error("a close later that tripped nothing reads quiet")
	}
	if state(t, newHandlers(t, nil, b)) {
		t.Error("no events: quiet")
	}
	if state(t, &Handlers{}) {
		t.Error("an unwired handler cannot claim a trip")
	}
}

// --- the tape ---

func TestTapeHasASparklineAndTheTriggersZ(t *testing.T) {
	n := 70
	all := fakeBars{}
	for _, s := range source.SweepUniverse {
		step := flat
		if s.Type == source.AssetTypeStock {
			step = alt
		}
		all[s.Symbol] = bars(s.Symbol, n, step)
	}
	// Anchor "now" on the last bar: LoadAsOf reads up to the day of its argument,
	// so serve bars up to the real clock by shifting the fixture to end today.
	shift := time.Now().UTC().Truncate(24 * time.Hour).Sub(day0.AddDate(0, 0, n-1))
	for sym, bs := range all {
		for i := range bs {
			bs[i].Time = bs[i].Time.Add(shift)
		}
		all[sym] = bs
	}
	h := newHandlers(t, nil, all)
	var body struct {
		AsOf    *string `json:"asOf"`
		Sensors []struct {
			Symbol string    `json:"symbol"`
			Closes []float64 `json:"closes"`
			Z      *float64  `json:"z"`
			Sigma  *float64  `json:"sigma"`
			Reason string    `json:"reason"`
		} `json:"sensors"`
	}
	if code := get(t, ledgerRouter(h), "/market/tape", &body); code != http.StatusOK {
		t.Fatalf("GET /market/tape = %d", code)
	}
	if len(body.Sensors) != 18 || body.AsOf == nil {
		t.Fatalf("sensors %d asOf %v", len(body.Sensors), body.AsOf)
	}
	for _, s := range body.Sensors {
		if len(s.Closes) != trigger.TapeWindow {
			t.Errorf("%s has %d closes, want %d", s.Symbol, len(s.Closes), trigger.TapeWindow)
		}
		if s.Z == nil || s.Sigma == nil || *s.Sigma <= 0 || s.Reason != "" {
			t.Errorf("%s z %v sigma %v reason %q", s.Symbol, s.Z, s.Sigma, s.Reason)
		}
	}
}

func TestTapeSaysWhyASensorHasNoZ(t *testing.T) {
	short := fakeBars{}
	for _, s := range source.SweepUniverse {
		bs := bars(s.Symbol, 30, alt)
		shift := time.Now().UTC().Truncate(24 * time.Hour).Sub(day0.AddDate(0, 0, 29))
		for i := range bs {
			bs[i].Time = bs[i].Time.Add(shift)
		}
		short[s.Symbol] = bs
	}
	h := newHandlers(t, nil, short)
	var body struct {
		Sensors []struct {
			Symbol string   `json:"symbol"`
			Z      *float64 `json:"z"`
			Reason string   `json:"reason"`
		} `json:"sensors"`
	}
	get(t, ledgerRouter(h), "/market/tape", &body)
	for _, s := range body.Sensors {
		if s.Z != nil || s.Reason != "insufficient_history" {
			t.Errorf("%s z %v reason %q; under sixty returns the tape says so", s.Symbol, s.Z, s.Reason)
		}
	}
}

// --- the deletion ---

func TestMoversIsGoneAndTheLedgerIsRouted(t *testing.T) {
	h := newHandlers(t, nil, fixtureBars())
	r := NewRouter(h, nil)
	if code := get(t, r, "/market/movers", nil); code != http.StatusNotFound {
		t.Errorf("GET /market/movers = %d, want 404: a top-gainers endpoint is an invitation to the widget the redesign removed", code)
	}
	for _, path := range []string{"/market/events", "/market/tape"} {
		if code := get(t, r, path, nil); code != http.StatusOK {
			t.Errorf("GET %s = %d", path, code)
		}
	}
	for _, rt := range r.Routes() {
		if strings.Contains(rt.Path, "movers") {
			t.Errorf("route %s still registered", rt.Path)
		}
	}
}
