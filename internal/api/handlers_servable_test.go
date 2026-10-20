package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
)

// fakeSymbols is the symbols table: two of the four simulator rows and two of
// the 28 sweep symbols.
type fakeSymbols map[string]bool

func newFakeSymbols() fakeSymbols {
	return fakeSymbols{"SIM:NOVA": true, "SIM:HELIX": true, "MUFG": true, "NVDA": true}
}

func (f fakeSymbols) Exists(_ context.Context, s string) (bool, error) { return f[s], nil }

func servableHandlers(src string) *Handlers {
	return &Handlers{Meta: newFakeSymbols(), Source: src, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func servableRouter(h *Handlers) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/symbols/:symbol", h.GetSymbolSnapshot)
	r.GET("/symbols/:symbol/candles", h.GetCandles)
	r.GET("/symbols/:symbol/indicators", h.GetIndicators)
	return r
}

// The 404 rule: a symbol is served when it is in the table, and a SIM: symbol
// only when this ticker runs the simulator.
func TestSymbolServable(t *testing.T) {
	cases := []struct {
		source, symbol string
		want           bool
	}{
		{"sim", "SIM:NOVA", true},
		{"sim", "MUFG", true}, // a real sweep symbol
		{"sim", "NOPE", false},
		{"finnhub", "MUFG", true},
		{"finnhub", "NVDA", true},
		{"finnhub", "SIM:NOVA", false}, // in the table, but the real feed has nothing behind it
		{"finnhub", "NOPE", false},
	}
	for _, c := range cases {
		got, err := servableHandlers(c.source).symbolServable(context.Background(), c.symbol)
		if err != nil || got != c.want {
			t.Errorf("source %s, %s: got (%v, %v), want %v", c.source, c.symbol, got, err, c.want)
		}
	}
}

func TestUnknownAndSimSymbolsAre404OnEveryRoute(t *testing.T) {
	for _, path := range []string{"/symbols/NOPE", "/symbols/NOPE/candles", "/symbols/NOPE/indicators"} {
		if code := get(t, servableRouter(servableHandlers("sim")), path, nil); code != http.StatusNotFound {
			t.Errorf("sim source: GET %s = %d, want 404", path, code)
		}
	}
	for _, path := range []string{"/symbols/SIM:NOVA", "/symbols/SIM:NOVA/candles", "/symbols/SIM:NOVA/indicators"} {
		if code := get(t, servableRouter(servableHandlers("finnhub")), path, nil); code != http.StatusNotFound {
			t.Errorf("finnhub source: GET %s = %d, want 404", path, code)
		}
	}
}
