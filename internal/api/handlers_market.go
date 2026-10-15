package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// GetMarketState handles GET /market/state (§8): whether the feed is live, closed
// as expected, stale, or down. It is the verdict the dead-man switch acts on, with
// its outcomes kept apart so the page's Market indicator can say unreachable only
// when the feed really is, and not when the market is simply shut. Phase 15 adds
// whether anything tripped.
func (h *Handlers) GetMarketState(c *gin.Context) {
	feed := "down"
	if h.Feed != nil {
		feed = h.Feed(c.Request.Context())
	}
	c.JSON(http.StatusOK, gin.H{"feed": feed})
}
