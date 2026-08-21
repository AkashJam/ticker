package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// GetHealthy handles GET /healthy (§8) — liveness only, no dependency
// checks (a load balancer / SSM health check just wants "is the process up").
func (h *Handlers) GetHealthy(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// GetReady handles GET /ready (§8) — readiness: Redis + Timescale
// connectivity. The sim source has nothing external to check (it's
// synthetic and in-process); a future live source (§16.1) would add its
// own check here.
func (h *Handlers) GetReady(c *gin.Context) {
	ctx := c.Request.Context()

	if err := h.TS.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready", "error": "timescale: " + err.Error()})
		return
	}
	if err := h.Redis.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready", "error": "redis: " + err.Error()})
		return
	}
	if h.Ready != nil {
		if err := h.Ready(); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready", "error": err.Error()})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
