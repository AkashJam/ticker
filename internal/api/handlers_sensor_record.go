package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/trigger"
)

// recordSeries is the sensor's daily closes and its benchmark's over the same
// sessions, as stored. The page divides each by the close on baseSession to draw
// both at 100; the figures it derives from are therefore in the payload.
type recordSeries struct {
	Dates          []string  `json:"dates"`
	Close          []float64 `json:"close"`
	BenchmarkClose []float64 `json:"benchmarkClose"` // null for a sensor with no benchmark
}

type recordResponse struct {
	Symbol          string        `json:"symbol"` // canonical, whichever form the request used
	Name            string        `json:"name"`
	Country         *eventCountry `json:"country"` // nil for a global name
	Theme           *string       `json:"theme"`
	Benchmark       *string       `json:"benchmark"`
	BenchmarkReason *string       `json:"benchmarkReason"`
	Thin            *eventThin    `json:"thin"`
	// BaseSession is the first session both lines have a close for: the 100.
	BaseSession      *string      `json:"baseSession"`
	Series           recordSeries `json:"series"`
	SessionsObserved int          `json:"sessionsObserved"`
	LatestSession    *string      `json:"latestSession"`
	// JudgedFrom is the first session the trigger tested, and SessionsJudged how
	// many of the series' sessions are on or after it: the denominator of any rate
	// of events. The series is longer, because the bars were seeded from 1 July.
	JudgedFrom     string `json:"judgedFrom"`
	SessionsJudged int    `json:"sessionsJudged"`
	EventCount     int    `json:"eventCount"`
	// Events are every trip the sensor has recorded, newest session first, in the
	// ledger's event shape without the band.
	Events []eventBody `json:"events"`
}

// recordMemos keeps one memo per sensor. The key is the canonical symbol, so the
// map cannot grow past the eighteen.
type recordMemos struct {
	mu sync.Mutex
	m  map[string]*atlasMemo
}

func (r *recordMemos) of(symbol string) *atlasMemo {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = map[string]*atlasMemo{}
	}
	if r.m[symbol] == nil {
		r.m[symbol] = &atlasMemo{}
	}
	return r.m[symbol]
}

// GetSensorRecord handles GET /market/sensors/{symbol} (§8): one sensor's
// accumulated record. {symbol} is a sensor's symbol in any case or its country's
// slug (/market/sensors/japan and /market/sensors/MUFG are one body); anything
// else, a SIM: symbol and a sector ETF included, is a 404.
func (h *Handlers) GetSensorRecord(c *gin.Context) {
	ctx := c.Request.Context()
	if h.Events == nil || h.Candles == nil || h.Ref == nil {
		h.Log.Error("api: sensor record: not wired")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "record unavailable"})
		return
	}
	lk, err := h.eventLookups(ctx)
	if err != nil {
		h.Log.Error("api: sensor record", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "record unavailable"})
		return
	}
	symbol, ok := lk.resolve(c.Param("symbol"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "unknown sensor"})
		return
	}
	body, err := h.records.of(symbol).get(time.Now(), func() ([]byte, error) { return h.buildRecord(ctx, lk, symbol) })
	if err != nil {
		h.Log.Error("api: sensor record", "symbol", symbol, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "record unavailable"})
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", body)
}

// resolve maps a request's name to a sensor symbol: a symbol in any case, or the
// slug of a country that has a sensor.
func (lk eventLookups) resolve(name string) (string, bool) {
	for symbol := range lk.bySensor {
		if strings.EqualFold(symbol, name) {
			return symbol, true
		}
	}
	for symbol, cn := range lk.byCountry {
		if strings.EqualFold(cn.Slug, name) {
			return symbol, true
		}
	}
	return "", false
}

func (h *Handlers) buildRecord(ctx context.Context, lk eventLookups, symbol string) ([]byte, error) {
	trips, err := h.Events.EventsFor(ctx, symbol)
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

	res := recordResponse{Symbol: symbol, Events: make([]eventBody, 0, len(trips))}
	for _, t := range trips {
		ev := lk.body(t, canary)
		ev.Price, _ = h.priceAndBand(ctx, t)
		res.Events = append(res.Events, ev)
	}
	res.EventCount = len(res.Events)

	sn := lk.bySensor[symbol]
	res.Name = sn.Name
	if cn, ok := lk.byCountry[symbol]; ok {
		res.Country = &eventCountry{Slug: cn.Slug, Name: cn.Country, Atlas: cn.Atlas}
	} else {
		res.Theme = sn.Theme
	}
	if sn.Liquidity != nil && *sn.Liquidity == "thin" && sn.LiquidityUsdM != nil && sn.LiquidityAsOf != nil {
		res.Thin = &eventThin{UsdM: *sn.LiquidityUsdM, AsOf: *sn.LiquidityAsOf, Label: "thin listing"}
	}
	if b, ok := trigger.Benchmarks[symbol]; ok {
		res.Benchmark = &b
	} else if why, ok := trigger.NoBenchmarkReason[symbol]; ok {
		res.BenchmarkReason = &why
	}

	if err := h.fillSeries(ctx, &res); err != nil {
		return nil, err
	}
	return json.Marshal(res)
}

// fillSeries reads every daily bar the sensor and its benchmark have and keeps
// the sessions both have a close for. A sensor with no benchmark keeps all of its
// own, and its benchmarkClose stays null.
func (h *Handlers) fillSeries(ctx context.Context, res *recordResponse) error {
	bars, err := h.Candles.Candles(ctx, res.Symbol, trigger.DailyInterval, time.Time{}, time.Time{})
	if err != nil {
		return fmt.Errorf("sensor record: closes: %w", err)
	}
	sym := trigger.NewCloses(bars)
	var bench trigger.Closes
	if res.Benchmark != nil {
		bb, err := h.Candles.Candles(ctx, *res.Benchmark, trigger.DailyInterval, time.Time{}, time.Time{})
		if err != nil {
			return fmt.Errorf("sensor record: benchmark closes: %w", err)
		}
		bench = trigger.NewCloses(bb)
	}

	res.Series = recordSeries{Dates: []string{}, Close: []float64{}}
	if bench != nil {
		res.Series.BenchmarkClose = []float64{}
	}
	for _, b := range bars {
		d := b.Time.UTC().Truncate(24 * time.Hour)
		if bench != nil {
			bc, ok := bench[d]
			if !ok {
				continue
			}
			res.Series.BenchmarkClose = append(res.Series.BenchmarkClose, bc)
		}
		res.Series.Dates = append(res.Series.Dates, d.Format(time.DateOnly))
		res.Series.Close = append(res.Series.Close, sym[d])
	}
	res.SessionsObserved = len(res.Series.Dates)
	res.JudgedFrom = trigger.LiveFrom.Format(time.DateOnly)
	for _, d := range res.Series.Dates {
		if d >= res.JudgedFrom {
			res.SessionsJudged++
		}
	}
	if n := res.SessionsObserved; n > 0 {
		first, last := res.Series.Dates[0], res.Series.Dates[n-1]
		res.BaseSession, res.LatestSession = &first, &last
	}
	return nil
}
