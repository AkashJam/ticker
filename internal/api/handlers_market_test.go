package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
)

func marketStateRouter(h *Handlers) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/market/state", h.GetMarketState)
	return r
}

func TestMarketStateReportsTheFeedAsGiven(t *testing.T) {
	for _, want := range []string{"live", "closed", "stale", "down"} {
		h := &Handlers{Feed: func(context.Context) string { return want }, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
		var got struct {
			Feed string `json:"feed"`
		}
		if code := get(t, marketStateRouter(h), "/market/state", &got); code != http.StatusOK {
			t.Fatalf("GET /market/state = %d", code)
		}
		if got.Feed != want {
			t.Errorf("feed = %q, want %q", got.Feed, want)
		}
	}
}

// A handler with no feed wired cannot vouch for it: down, not a panic.
func TestMarketStateWithNoFeedIsDown(t *testing.T) {
	var got struct {
		Feed string `json:"feed"`
	}
	if code := get(t, marketStateRouter(&Handlers{}), "/market/state", &got); code != http.StatusOK || got.Feed != "down" {
		t.Errorf("code %d, feed %q, want 200 and down", code, got.Feed)
	}
}
