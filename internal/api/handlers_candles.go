package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/source"
)

type candlesResponse struct {
	Symbol   string          `json:"symbol"`
	Interval string          `json:"interval"`
	Candles  []source.Candle `json:"candles"`
}

// GetCandles handles GET /symbols/{symbol}/candles?interval=1m&from=...&to=...
// (§8). interval defaults to "1m"; from/to are RFC3339, both optional.
func (h *Handlers) GetCandles(c *gin.Context) {
	symbol := c.Param("symbol")
	interval := c.DefaultQuery("interval", "1m")

	from, ok := parseOptionalTime(c, "from")
	if !ok {
		return
	}
	to, ok := parseOptionalTime(c, "to")
	if !ok {
		return
	}

	candles, err := h.TS.Candles(c.Request.Context(), symbol, interval, from, to)
	if err != nil {
		h.Log.Error("api: get candles", "symbol", symbol, "interval", interval, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	c.JSON(http.StatusOK, candlesResponse{Symbol: symbol, Interval: interval, Candles: candles})
}

// parseOptionalTime reads an RFC3339 query parameter, returning the zero
// time (meaning "unbounded") if absent. Writes a 400 response and returns
// ok=false on a malformed value.
func parseOptionalTime(c *gin.Context, param string) (t time.Time, ok bool) {
	raw := c.Query(param)
	if raw == "" {
		return time.Time{}, true
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": param + " must be RFC3339"})
		return time.Time{}, false
	}
	return parsed, true
}
