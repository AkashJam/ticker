package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func limited(t *testing.T, h gin.HandlerFunc, n int) (ok, tooMany int) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(h)
	r.GET("/", func(c *gin.Context) { c.Status(http.StatusOK) })
	for i := 0; i < n; i++ {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
		req.RemoteAddr = "172.18.0.5:41234" // the portfolio container: every visitor at once
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		switch w.Code {
		case http.StatusOK:
			ok++
		case http.StatusTooManyRequests:
			tooMany++
		}
	}
	return ok, tooMany
}

// One /market render is about thirty reads. The first limits (burst 100) were
// exhausted by three renders landing together, which blanked every price in CI.
func TestRateLimit_ConcurrentMarketRendersAreServed(t *testing.T) {
	const render = 30
	ok, tooMany := limited(t, RateLimit(), 10*render)
	if tooMany != 0 {
		t.Fatalf("ten renders at once: %d of %d reads rate-limited, want none", tooMany, ok+tooMany)
	}
}

func TestRateLimit_StillStopsARunaway(t *testing.T) {
	ok, tooMany := limited(t, RateLimit(), 5000)
	if tooMany == 0 {
		t.Fatal("5000 reads in an instant, none limited")
	}
	if ok < rateLimitBurst {
		t.Fatalf("served %d, want at least the burst of %d", ok, rateLimitBurst)
	}
}
