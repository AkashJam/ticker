package api

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/store"
)

// There is no GET /symbols: GET /sensors lists the eighteen, and the list of
// tracked symbols it replaced was removed with the tracked column (phase17.md
// step 4, decision 7, migration 000009).

type snapshotResponse struct {
	Symbol        string  `json:"symbol"`
	Name          string  `json:"name"`
	Price         float64 `json:"price"`
	Change        float64 `json:"change"`
	ChangePercent float64 `json:"changePercent"`
	DayHigh       float64 `json:"dayHigh"`
	DayLow        float64 `json:"dayLow"`
	PrevClose     float64 `json:"prevClose"`
	// AsOf is the price's vintage: the last trade, or the latest daily bar
	// when nothing is cached. Null when the service has neither (ADR-006).
	AsOf *time.Time `json:"asOf"`
}

// GetSymbolSnapshot handles GET /symbols/{symbol} (§8).
func (h *Handlers) GetSymbolSnapshot(c *gin.Context) {
	symbol := c.Param("symbol")
	if !h.requireSymbol(c, symbol) {
		return
	}
	snap, err := h.snapshotFor(c.Request.Context(), symbol)
	if err != nil {
		h.Log.Error("api: snapshot", "symbol", symbol, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.JSON(http.StatusOK, snap)
}

// snapshotFor combines the live quote cache (current price) with the daily
// candles (the previous close, and the day's high and low) — the one place a
// symbol's current standing is computed.
func (h *Handlers) snapshotFor(ctx context.Context, symbol string) (snapshotResponse, error) {
	// Most recent 2 daily candles, oldest first. The sweep writes a session's
	// bar at 16:15 New York, so until then the newest one is the previous
	// session's, not today's.
	dailyCandles, err := h.TS.RecentCandles(ctx, symbol, "1d", 2)
	if err != nil {
		return snapshotResponse{}, err
	}

	var cached *source.NormalizedTick
	if raw, err := h.Redis.CacheGet(ctx, store.QuoteCacheKey(symbol)); err == nil {
		var tick source.NormalizedTick
		if json.Unmarshal([]byte(raw), &tick) == nil {
			cached = &tick
		}
	}

	today, prev := pickDay(dailyCandles, cached)
	if today == nil && len(dailyCandles) > 0 {
		// The newest bar is the previous close, so the day's range is read
		// from the session's live bars.
		bars, err := h.TS.Candles(ctx, symbol, liveRangeInterval, sessionStart(cached.Timestamp), time.Time{})
		if err != nil {
			return snapshotResponse{}, err
		}
		today = liveDay(bars)
	}
	return buildSnapshot(symbol, cached, today, prev), nil
}

// liveRangeInterval is the bar size the day's high and low are read from while
// the session's daily bar does not exist yet.
const liveRangeInterval = "5m"

var newYork = func() *time.Location {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		panic(err) // the binary embeds time/tzdata (cmd/main.go)
	}
	return loc
}()

// sessionDate is the New York calendar date of t, as midnight UTC: the form
// the daily bars' time is stored in (sweep.go).
func sessionDate(t time.Time) time.Time {
	y, m, d := t.In(newYork).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// sessionStart is midnight New York on the date of t.
func sessionStart(t time.Time) time.Time {
	y, m, d := t.In(newYork).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, newYork)
}

// pickDay says which of the (up to two, oldest first) daily bars is today's
// and which is the previous close. The last trade's New York date decides: if
// the newest bar carries it the sweep has already written today's bar, so the
// bar before it is the previous close; otherwise the newest bar is the
// previous close and there is no daily bar for today yet (today is nil). With
// no cached trade there is no session to compare against, so the newest bar
// stands in for today, as before.
func pickDay(daily []source.Candle, cached *source.NormalizedTick) (today, prev *source.Candle) {
	switch len(daily) {
	case 0:
		return nil, nil
	case 1:
		if cached != nil && !sessionDate(cached.Timestamp).Equal(daily[0].Time.UTC()) {
			return nil, &daily[0]
		}
		return &daily[0], nil
	}
	if cached != nil && !sessionDate(cached.Timestamp).Equal(daily[1].Time.UTC()) {
		return nil, &daily[1]
	}
	return &daily[1], &daily[0]
}

// liveDay folds a session's intraday bars into the one candle the snapshot
// reads (high and low); nil when there are none.
func liveDay(bars []source.Candle) *source.Candle {
	if len(bars) == 0 {
		return nil
	}
	day := source.Candle{High: bars[0].High, Low: bars[0].Low, Time: bars[0].Time}
	for _, b := range bars[1:] {
		day.High = math.Max(day.High, b.High)
		day.Low = math.Min(day.Low, b.Low)
	}
	return &day
}

// buildSnapshot is snapshotFor without the I/O. Price and its vintage come
// from the cached live tick, else from the latest daily bar; with neither the
// vintage is null, which the page shows as unreachable rather than inventing
// a time (ADR-006).
func buildSnapshot(symbol string, cached *source.NormalizedTick, today, prev *source.Candle) snapshotResponse {
	price, asOf := 0.0, time.Time{}
	switch {
	case cached != nil:
		price, asOf = cached.Price, cached.Timestamp
	case today != nil:
		price, asOf = today.Close, today.Time
	}

	prevClose := 0.0
	if prev != nil {
		prevClose = prev.Close
	}
	change, changePercent := 0.0, 0.0
	if prevClose != 0 {
		change = price - prevClose
		changePercent = change / prevClose * 100
	}

	dayHigh, dayLow := price, price
	if today != nil {
		dayHigh, dayLow = today.High, today.Low
		if cached != nil {
			dayHigh, dayLow = math.Max(dayHigh, price), math.Min(dayLow, price)
		}
	}

	var asOfPtr *time.Time
	if !asOf.IsZero() {
		asOfPtr = &asOf
	}

	meta := symbolMetaOrFallback(symbol)
	return snapshotResponse{
		Symbol: symbol, Name: meta.Name,
		Price: round2(price), Change: round2(change), ChangePercent: round2(changePercent),
		DayHigh: round2(dayHigh), DayLow: round2(dayLow), PrevClose: round2(prevClose),
		AsOf: asOfPtr,
	}
}
