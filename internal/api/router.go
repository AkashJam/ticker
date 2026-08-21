package api

import (
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/AkashJam/ticker/internal/sse"
)

// NewRouter builds the Gin engine covering every route in §8. Mounted
// internal-only (Option B): reached over the Docker network by Next.js,
// never given a public route of its own.
func NewRouter(h *Handlers, hub *sse.Hub) *gin.Engine {
	r := gin.New()
	r.Use(RequestLogger(h.Log), gin.Recovery(), RateLimit())

	r.GET("/healthy", h.GetHealthy)
	r.GET("/ready", h.GetReady)
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.GET("/symbols", h.GetSymbols)
	r.GET("/symbols/:symbol", h.GetSymbolSnapshot)
	r.GET("/symbols/:symbol/candles", h.GetCandles)
	r.GET("/symbols/:symbol/indicators", h.GetIndicators)
	r.GET("/market/movers", h.GetMovers)
	r.GET("/col", h.GetCostOfLiving)
	r.GET("/col/:city", h.GetCostOfLivingCity)

	r.GET("/stream", func(c *gin.Context) { hub.ServeHTTP(c.Writer, c.Request) })

	return r
}
