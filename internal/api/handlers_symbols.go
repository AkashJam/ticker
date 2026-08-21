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

type symbolResponse struct {
	Symbol   string `json:"symbol"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Exchange string `json:"exchange"`
}

// GetSymbols handles GET /symbols (§8).
func (h *Handlers) GetSymbols(c *gin.Context) {
	symbols, err := h.Meta.Symbols(c.Request.Context())
	if err != nil {
		h.Log.Error("api: get symbols", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	out := make([]symbolResponse, len(symbols))
	for i, s := range symbols {
		out[i] = symbolResponse{Symbol: s.Symbol, Name: s.Name, Type: string(s.Type), Exchange: s.Exchange}
	}
	c.JSON(http.StatusOK, out)
}

type snapshotResponse struct {
	Symbol        string    `json:"symbol"`
	Name          string    `json:"name"`
	Price         float64   `json:"price"`
	Change        float64   `json:"change"`
	ChangePercent float64   `json:"changePercent"`
	DayHigh       float64   `json:"dayHigh"`
	DayLow        float64   `json:"dayLow"`
	PrevClose     float64   `json:"prevClose"`
	UpdatedAt     time.Time `json:"updatedAt"`
	Simulated     bool      `json:"simulated"`
}

// GetSymbolSnapshot handles GET /symbols/{symbol} (§8).
func (h *Handlers) GetSymbolSnapshot(c *gin.Context) {
	symbol := c.Param("symbol")
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
// change/changePercent) — shared by GetSymbolSnapshot and GetMovers so
// both compute a symbol's current standing the same way.
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

	price, updatedAt, simulated := 0.0, time.Time{}, true
	if raw, err := h.Redis.CacheGet(ctx, store.QuoteCacheKey(symbol)); err == nil {
		var tick source.NormalizedTick
		if jsonErr := json.Unmarshal([]byte(raw), &tick); jsonErr == nil {
			price, updatedAt, simulated = tick.Price, tick.Timestamp, tick.Simulated
		}
	} else if today != nil {
		price, updatedAt = today.Close, today.Time
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

	meta := symbolMetaOrFallback(symbol)
	return snapshotResponse{
		Symbol: symbol, Name: meta.Name,
		Price: round2(price), Change: round2(change), ChangePercent: round2(changePercent),
		DayHigh: round2(dayHigh), DayLow: round2(dayLow), PrevClose: round2(prevClose),
		UpdatedAt: updatedAt, Simulated: simulated,
	}, nil
}
