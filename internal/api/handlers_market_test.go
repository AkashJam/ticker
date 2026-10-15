package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

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

type marketStateBody struct {
	Feed    string  `json:"feed"`
	Session string  `json:"session"`
	OpensAt *string `json:"opensAt"`
}

// No session wired (the simulator's server wires one that is always open, and a
// bare handler has none) reads open with no opening time.
func TestMarketStateSessionDefaultsToOpen(t *testing.T) {
	var got marketStateBody
	get(t, marketStateRouter(&Handlers{}), "/market/state", &got)
	if got.Session != "open" || got.OpensAt != nil {
		t.Errorf("session %q opensAt %v, want open and null", got.Session, got.OpensAt)
	}
}

func TestMarketStateClosedCarriesTheNextOpenInUTC(t *testing.T) {
	// Monday 19 October 2026, 09:30 in New York (EDT, UTC-4).
	opens := time.Date(2026, time.October, 19, 9, 30, 0, 0, time.FixedZone("EDT", -4*3600))
	h := &Handlers{
		Feed:    func(context.Context) string { return "closed" },
		Session: func(context.Context) (bool, time.Time) { return true, opens },
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	var got marketStateBody
	if code := get(t, marketStateRouter(h), "/market/state", &got); code != http.StatusOK {
		t.Fatalf("GET /market/state = %d", code)
	}
	if got.Session != "closed" || got.OpensAt == nil || *got.OpensAt != "2026-10-19T13:30:00Z" {
		t.Errorf("session %q opensAt %v, want closed and 2026-10-19T13:30:00Z", got.Session, got.OpensAt)
	}
}

func TestMarketStateOpenHasNoOpeningTime(t *testing.T) {
	h := &Handlers{Session: func(context.Context) (bool, time.Time) { return false, time.Time{} }}
	var got marketStateBody
	get(t, marketStateRouter(h), "/market/state", &got)
	if got.Session != "open" || got.OpensAt != nil {
		t.Errorf("session %q opensAt %v, want open and null", got.Session, got.OpensAt)
	}
}
