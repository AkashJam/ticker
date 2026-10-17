package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// GetMarketState handles GET /market/state (§8): the feed's state and the regular
// session's. feed is the verdict the dead-man switch acts on, with its outcomes
// kept apart so the page's Market indicator says unreachable only when the feed
// really is. session says whether the regular session is open and, when it is
// closed, opensAt is the next open, so the page can freeze its prices and count
// down to it (Phase 14 step 7). tripped says whether the last close tripped
// something: the newest event is from the newest session with bars (Phase 15
// step 7), so it clears itself after the next close that trips nothing.
func (h *Handlers) GetMarketState(c *gin.Context) {
	ctx := c.Request.Context()

	feed := "down"
	if h.Feed != nil {
		feed = h.Feed(ctx)
	}

	session, opensAt := "open", (*string)(nil)
	if h.Session != nil {
		if closed, at := h.Session(ctx); closed {
			session = "closed"
			if !at.IsZero() {
				s := at.UTC().Format(time.RFC3339)
				opensAt = &s
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"feed": feed, "session": session, "opensAt": opensAt, "tripped": h.tripped(ctx)})
}
