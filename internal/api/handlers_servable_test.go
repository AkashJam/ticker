package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/source"
)

// fakeSymbols is the symbols table: the four simulator rows (tracked) and two
// of the 28 sweep symbols (untracked until the release).
type fakeSymbols struct {
	all     map[string]bool // symbol -> tracked
	listing []source.SymbolInfo
}

func newFakeSymbols() *fakeSymbols {
	f := &fakeSymbols{all: map[string]bool{"SIM:NOVA": true, "SIM:HELIX": true, "MUFG": false, "NVDA": false}}
	f.listing = []source.SymbolInfo{{Symbol: "SIM:HELIX"}, {Symbol: "SIM:NOVA"}}
	return f
}

func (f *fakeSymbols) Symbols(context.Context) ([]source.SymbolInfo, error) { return f.listing, nil }
func (f *fakeSymbols) Exists(_ context.Context, s string) (bool, error) {
	_, ok := f.all[s]
	return ok, nil
}

func servableHandlers(src string) *Handlers {
	return &Handlers{Meta: newFakeSymbols(), Source: src, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func servableRouter(h *Handlers) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/symbols", h.GetSymbols)
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
		{"sim", "MUFG", true}, // a real, untracked sweep symbol
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

func TestSymbolsListHidesSimOnTheRealFeed(t *testing.T) {
	var sim, real []symbolResponse
	get(t, servableRouter(servableHandlers("sim")), "/symbols", &sim)
	get(t, servableRouter(servableHandlers("finnhub")), "/symbols", &real)
	if len(sim) != 2 {
		t.Errorf("sim source lists %d symbols, want 2", len(sim))
	}
	if real == nil || len(real) != 0 {
		t.Errorf("finnhub source lists %v, want an empty (non-null) list: the SIM: rows are not served there", real)
	}
}
