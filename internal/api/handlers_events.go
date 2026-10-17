package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/trigger"
)

// eventsLimit caps the ledger. At one or two trips a week it is about two years;
// past it the response says it was cut, rather than silently dropping the past
// (phase15.md step 7, decision 5).
const eventsLimit = 100

// EventReader is what the ledger reads from the events table.
type EventReader interface {
	Events(ctx context.Context, limit int) ([]trigger.Trip, error)
	// EventsFor is one symbol's whole record, newest session first.
	EventsFor(ctx context.Context, symbol string) ([]trigger.Trip, error)
}

type eventCountry struct {
	Slug  string `json:"slug"`
	Name  string `json:"name"`
	Atlas bool   `json:"atlas"` // false for Colombia and Peru: a row in the table, none on /atlas
}

type eventPrice struct {
	Close         float64 `json:"close"`
	PrevClose     float64 `json:"prevClose"`
	Change        float64 `json:"change"`
	ChangePercent float64 `json:"changePercent"`
}

type eventTrigger struct {
	SymbolReturn    float64  `json:"symbolReturn"`
	Benchmark       *string  `json:"benchmark"`
	BenchmarkReturn *float64 `json:"benchmarkReturn"`
	BenchmarkReason *string  `json:"benchmarkReason"`
	Residual        float64  `json:"residual"`
	Sigma           float64  `json:"sigma"`
	Z               float64  `json:"z"`
	Threshold       float64  `json:"threshold"`
	Window          int      `json:"window"`
}

type eventThin struct {
	UsdM  float64 `json:"usdM"`
	AsOf  string  `json:"asOf"`
	Label string  `json:"label"`
}

// eventBody is an event without its band: the ledger adds the band, the
// sensor's record (which draws the longer series instead) does not.
type eventBody struct {
	// ID is the fragment the entry is addressed by: /market#japan-2026-10-15.
	ID          string        `json:"id"`
	Symbol      string        `json:"symbol"`
	Name        string        `json:"name"`
	SessionDate string        `json:"sessionDate"`
	ClosedAt    time.Time     `json:"closedAt"`
	SessionsAgo int           `json:"sessionsAgo"` // sessions since this one, by the canary's bars
	Country     *eventCountry `json:"country"`     // nil for a global name
	Theme       *string       `json:"theme"`       // a global name's, else nil
	Price       *eventPrice   `json:"price"`
	Trigger     eventTrigger  `json:"trigger"`
	// Split and Coverage are stored JSON, passed through whole. Split is null
	// until the currency split has been made; Coverage until the first fetch.
	Split    json.RawMessage `json:"split"`
	Coverage json.RawMessage `json:"coverage"`
	Caveats  []string        `json:"caveats"`
	Thin     *eventThin      `json:"thin"`
}

type eventResponse struct {
	eventBody
	Band *trigger.Band `json:"band"`
}

type eventsResponse struct {
	Events        []eventResponse `json:"events"`
	Truncated     bool            `json:"truncated"`
	LatestSession *string         `json:"latestSession"` // the newest session with bars, or nil
}

// GetEvents handles GET /market/events (§8): the ledger. Every event the trigger
// recorded, newest session first and never ranked, each with its country, price
// move, the trigger's own figures, the expectation band, the currency split, the
// news fetched and the caveats that apply.
func (h *Handlers) GetEvents(c *gin.Context) {
	body, err := h.events.get(time.Now(), func() ([]byte, error) { return h.buildEvents(c.Request.Context()) })
	if err != nil {
		h.Log.Error("api: events", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "ledger unavailable"})
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", body)
}

func (h *Handlers) buildEvents(ctx context.Context) ([]byte, error) {
	if h.Events == nil || h.Candles == nil || h.Ref == nil {
		return nil, fmt.Errorf("the ledger is not wired")
	}
	trips, err := h.Events.Events(ctx, eventsLimit+1)
	if err != nil {
		return nil, err
	}
	res := eventsResponse{Events: []eventResponse{}}
	if len(trips) > eventsLimit {
		trips, res.Truncated = trips[:eventsLimit], true
	}

	lk, err := h.eventLookups(ctx)
	if err != nil {
		return nil, err
	}

	var oldest time.Time
	if len(trips) > 0 {
		oldest = trips[len(trips)-1].SessionDate
	}
	canary, err := h.canarySessions(ctx, oldest)
	if err != nil {
		return nil, err
	}
	if n := len(canary); n > 0 {
		s := canary[n-1].Format(time.DateOnly)
		res.LatestSession = &s
	}

	for _, t := range trips {
		ev := eventResponse{eventBody: lk.body(t, canary)}
		ev.Price, ev.Band = h.priceAndBand(ctx, t)
		res.Events = append(res.Events, ev)
	}
	return json.Marshal(res)
}

// priceAndBand reads the sensor's closes up to the session and returns the
// session's price move and the expectation band. Either is nil when the bars
// cannot give it; the entry then simply lacks that block.
func (h *Handlers) priceAndBand(ctx context.Context, t trigger.Trip) (*eventPrice, *trigger.Band) {
	symbols := []string{t.Symbol}
	if t.Benchmark != nil {
		symbols = append(symbols, *t.Benchmark)
	}
	all, err := trigger.LoadAsOf(ctx, h.Candles, symbols, t.SessionDate)
	if err != nil {
		h.Log.Warn("api: events: closes", "symbol", t.Symbol, "error", err)
		return nil, nil
	}
	sessions := trigger.Sessions(all)
	at := -1
	for i, s := range sessions {
		if s.Equal(t.SessionDate) {
			at = i
		}
	}
	if at < 1 {
		return nil, nil
	}
	sym := all[t.Symbol]
	var price *eventPrice
	cl, okC := sym[sessions[at]]
	prev, okP := sym[sessions[at-1]]
	if okC && okP && prev != 0 {
		price = &eventPrice{Close: cl, PrevClose: prev, Change: round2(cl - prev), ChangePercent: round2((cl/prev - 1) * 100)}
	}
	var bench trigger.Closes
	if t.Benchmark != nil {
		bench = all[*t.Benchmark]
	}
	band, err := trigger.BuildBand(sessions, at, sym, bench, t.Sigma, t.Threshold)
	if err != nil {
		return price, nil
	}
	return price, &band
}

// canarySessions are the dates of the canary sensor's daily bars since from, the
// calendar "sessions ago" is counted on.
func (h *Handlers) canarySessions(ctx context.Context, from time.Time) ([]time.Time, error) {
	if from.IsZero() {
		from = time.Now().UTC().AddDate(0, 0, -30)
	}
	bars, err := h.Candles.Candles(ctx, source.SweepUniverse[0].Symbol, trigger.DailyInterval, from.AddDate(0, 0, -1), time.Time{})
	if err != nil {
		return nil, err
	}
	out := make([]time.Time, 0, len(bars))
	for _, b := range bars {
		out = append(out, b.Time.UTC())
	}
	return out, nil
}

// sessionsAfter counts the sessions strictly after session.
func sessionsAfter(sessions []time.Time, session time.Time) int {
	n := 0
	for _, s := range sessions {
		if s.After(session) {
			n++
		}
	}
	return n
}

func rawOrNull(b []byte) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("null")
	}
	return json.RawMessage(b)
}

// tripped reports whether the newest event is from the newest session with
// bars: the last close tripped something (phase15.md step 7, decision 4). It
// reads quiet again after the next close that trips nothing.
func (h *Handlers) tripped(ctx context.Context) bool {
	if h.Events == nil || h.Candles == nil {
		return false
	}
	latest, err := h.Events.Events(ctx, 1)
	if err != nil || len(latest) == 0 {
		return false
	}
	sessions, err := h.canarySessions(ctx, latest[0].SessionDate)
	if err != nil || len(sessions) == 0 {
		return false
	}
	return sessions[len(sessions)-1].Equal(latest[0].SessionDate.UTC())
}

// eventLookups are the reference rows an event is dressed from.
type eventLookups struct {
	bySensor  map[string]reference.Sensor
	byCountry map[string]reference.Country // keyed by the sensor's symbol
}

func (h *Handlers) eventLookups(ctx context.Context) (eventLookups, error) {
	sensors, err := h.Ref.Sensors(ctx)
	if err != nil {
		return eventLookups{}, err
	}
	countries, err := h.Ref.Countries(ctx)
	if err != nil {
		return eventLookups{}, err
	}
	lk := eventLookups{bySensor: map[string]reference.Sensor{}, byCountry: map[string]reference.Country{}}
	for _, s := range sensors {
		lk.bySensor[s.Symbol] = s
	}
	for _, cn := range countries {
		if cn.Sensor != nil {
			lk.byCountry[*cn.Sensor] = cn
		}
	}
	return lk, nil
}

// body dresses a trip as the API serves it, without its price or band: the
// trigger's own figures, the stored split and coverage, the country or theme and
// the thin-listing line. canary is the calendar "sessions ago" is counted on.
func (lk eventLookups) body(t trigger.Trip, canary []time.Time) eventBody {
	ev := eventBody{
		Symbol: t.Symbol, SessionDate: t.SessionDate.Format(time.DateOnly), ClosedAt: t.ClosedAt.UTC(),
		SessionsAgo: sessionsAfter(canary, t.SessionDate),
		Trigger: eventTrigger{
			SymbolReturn: t.SymbolReturn, Benchmark: t.Benchmark, BenchmarkReturn: t.BenchmarkReturn, BenchmarkReason: t.BenchmarkReason,
			Residual: t.Residual, Sigma: t.Sigma, Z: t.Z, Threshold: t.Threshold, Window: t.WindowSessions,
		},
		Split: rawOrNull(t.Split), Coverage: rawOrNull(t.Coverage), Caveats: t.Caveats,
	}
	if ev.Caveats == nil {
		ev.Caveats = []string{}
	}
	sn := lk.bySensor[t.Symbol]
	ev.Name = sn.Name
	if ev.Name == "" {
		ev.Name = t.Symbol
	}
	ev.ID = strings.ToLower(t.Symbol)
	if cn, ok := lk.byCountry[t.Symbol]; ok {
		ev.Country = &eventCountry{Slug: cn.Slug, Name: cn.Country, Atlas: cn.Atlas}
		ev.ID = cn.Slug
	} else {
		ev.Theme = sn.Theme
	}
	ev.ID += "-" + ev.SessionDate
	if sn.Liquidity != nil && *sn.Liquidity == "thin" && sn.LiquidityUsdM != nil && sn.LiquidityAsOf != nil {
		ev.Thin = &eventThin{UsdM: *sn.LiquidityUsdM, AsOf: *sn.LiquidityAsOf, Label: "thin listing"}
	}
	return ev
}
