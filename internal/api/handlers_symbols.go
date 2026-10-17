package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/store"
)

type symbolResponse struct {
	Symbol   string `json:"symbol"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Exchange string `json:"exchange"`
	// Regime surfaces source.SymbolInfo.Regime (steady/high_vol/
	// mean_reverting/gappy) — the sim source's per-symbol behavior label,
	// previously computed but never serialized. Used by the portfolio
	// site's market dashboard to badge each card (mockups/v3).
	Regime string `json:"regime"`
}

// GetSymbols handles GET /symbols (§8).
func (h *Handlers) GetSymbols(c *gin.Context) {
	symbols, err := h.Meta.Symbols(c.Request.Context())
	if err != nil {
		h.Log.Error("api: get symbols", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	// Non-nil, so an empty list is [] and not null.
	out := make([]symbolResponse, 0, len(symbols))
	for _, s := range symbols {
		if strings.HasPrefix(s.Symbol, simPrefix) && !h.simServed() {
			continue // the table keeps the SIM: rows; the real feed does not list them
		}
		out = append(out, symbolResponse{
			Symbol: s.Symbol, Name: s.Name, Type: string(s.Type), Exchange: s.Exchange,
			Regime: string(s.Regime),
		})
	}
	c.JSON(http.StatusOK, out)
}

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
