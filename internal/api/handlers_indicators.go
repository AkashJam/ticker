package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/indicators"
)

type indicatorsResponse struct {
	Symbol     string                      `json:"symbol"`
	Interval   string                      `json:"interval"`
	Indicators map[string]indicators.Value `json:"indicators"`
	Cached     bool                        `json:"cached"`
}

// indicatorSeriesResponse is the `series=true` shape (§8) — one value per
// candle instead of just the latest, e.g. for a chart line.
type indicatorSeriesResponse struct {
	Symbol   string                              `json:"symbol"`
	Interval string                              `json:"interval"`
	Series   map[string][]indicators.SeriesPoint `json:"series"`
	Cached   bool                                `json:"cached"`
}

// GetIndicators handles GET /symbols/{symbol}/indicators?set=ema,rsi&interval=1h
// (§8) — the compute-on-read + cache path (§9.4). Add `series=true` for a
// full series (one value per candle) instead of just the latest value —
// what the frontend's charts need (portfolio.md §15 Phase C).
func (h *Handlers) GetIndicators(c *gin.Context) {
	symbol := c.Param("symbol")
	interval := c.DefaultQuery("interval", "1h")
	setParam := c.DefaultQuery("set", "ema,rsi")
	series := c.Query("series") == "true"

	var names []string
	for _, n := range strings.Split(setParam, ",") {
		if n = strings.TrimSpace(n); n != "" {
			names = append(names, n)
		}
	}
	if len(names) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "set must name at least one indicator"})
		return
	}

	ctx := c.Request.Context()
	candles, err := h.TS.RecentCandles(ctx, symbol, interval, indicatorHistoryLimit)
	if err != nil {
		h.Log.Error("api: get indicators: recent candles", "symbol", symbol, "interval", interval, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	if series {
		points, cached, err := h.Cache.GetSeries(ctx, symbol, interval, names, candles)
		if err != nil {
			h.Log.Error("api: get indicators: compute series", "symbol", symbol, "interval", interval, "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
		c.JSON(http.StatusOK, indicatorSeriesResponse{Symbol: symbol, Interval: interval, Series: points, Cached: cached})
		return
	}

	values, cached, err := h.Cache.Get(ctx, symbol, interval, names, candles)
	if err != nil {
		h.Log.Error("api: get indicators: compute", "symbol", symbol, "interval", interval, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	c.JSON(http.StatusOK, indicatorsResponse{Symbol: symbol, Interval: interval, Indicators: values, Cached: cached})
}
