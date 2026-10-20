package api

import (
	"context"
	"encoding/json"
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
// candle (day high/low and, from the prior day's close,
// change/changePercent) — the one place a symbol's current standing is
// computed.
func (h *Handlers) snapshotFor(ctx context.Context, symbol string) (snapshotResponse, error) {
	// Most recent 2 daily candles, oldest first: [yesterday, today] once
	// the service has run past midnight once; just [today] on day one.
	dailyCandles, err := h.TS.RecentCandles(ctx, symbol, "1d", 2)
	if err != nil {
		return snapshotResponse{}, err
	}

	var today, prev *source.Candle
	switch len(dailyCandles) {
	case 1:
		today = &dailyCandles[0]
	case 2:
		prev, today = &dailyCandles[0], &dailyCandles[1]
	}

	var cached *source.NormalizedTick
	if raw, err := h.Redis.CacheGet(ctx, store.QuoteCacheKey(symbol)); err == nil {
		var tick source.NormalizedTick
		if json.Unmarshal([]byte(raw), &tick) == nil {
			cached = &tick
		}
	}
	return buildSnapshot(symbol, cached, today, prev), nil
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
