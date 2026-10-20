// Package api implements the Gin REST surface (portfolio.md §8) — internal
// only, no public route (Option B): reached exclusively over the Docker
// network by the Next.js server, never directly by a browser.
package api

import (
	"context"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/indicators"
	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/store"
	"github.com/AkashJam/ticker/internal/trigger"
)

const indicatorHistoryLimit = 200

// Handlers holds every dependency the route handlers need. A plain struct
// with method-per-endpoint (handlers_*.go) rather than per-endpoint free
// functions with closures — keeps the dependency list visible in one place.
type Handlers struct {
	Meta SymbolStore
	// Source is the ticker's --source, "sim" or "finnhub". It decides whether
	// SIM: symbols exist at all: only the simulator serves them (ADR-006).
	Source string
	// Feed reports the ingest feed's state for GET /market/state: "live",
	// "closed", "stale" or "down". Nil answers "down".
	Feed func(ctx context.Context) string
	// Session reports whether the regular session is closed and, if so, when it
	// next opens. Nil reads open.
	Session func(ctx context.Context) (closed bool, opensAt time.Time)
	TS      *store.Timescale
	// Events and Candles feed the ledger and the tape (phase15.md step 7). Both
	// are the Timescale store in production and fakes in tests.
	Events  EventReader
	Candles trigger.RangeReader
	Redis   *store.Redis
	Cache   *indicators.Cache
	Ready   func() error // aggregate readiness check (§8: GET /ready) — see server.go
	Log     *slog.Logger
	// Ref serves /atlas/cities from the reference datasets, falling back to the
	// embedded snapshots when Redis cannot answer.
	Ref   *reference.Store
	atlas memo
	// sensors reuses the memo: the list changes with a deploy, not a request.
	sensors memo
	events  memo
	tape    memo
	records memoSet
	cities  memoSet
}

// SymbolStore is what the symbol handlers need from the symbols table, as an
// interface so the 404 rule can be tested without a database.
type SymbolStore interface {
	Symbols(ctx context.Context) ([]source.SymbolInfo, error)
	Exists(ctx context.Context, symbol string) (bool, error)
}

func NewHandlers(meta *store.Meta, ts *store.Timescale, redis *store.Redis, cache *indicators.Cache, ready func() error, log *slog.Logger) *Handlers {
	return &Handlers{Meta: meta, TS: ts, Events: ts, Candles: ts, Redis: redis, Cache: cache, Ready: ready, Log: log}
}

const simPrefix = "SIM:"

// simServed is whether SIM: symbols are served at all. They are the simulator's
// by construction, so on the real feed a SIM: URL has nothing behind it.
func (h *Handlers) simServed() bool { return h.Source == "sim" }

// symbolServable is the 404 rule: a symbol is served when it is in the table,
// and a SIM: symbol only when this ticker runs the simulator. On any miss the
// handler answers 404, so the portfolio's notFound() path does the rest.
func (h *Handlers) symbolServable(ctx context.Context, symbol string) (bool, error) {
	if strings.HasPrefix(symbol, simPrefix) && !h.simServed() {
		return false, nil
	}
	return h.Meta.Exists(ctx, symbol)
}

// requireSymbol writes the 404 (or 500) and returns false when the symbol is
// not servable.
func (h *Handlers) requireSymbol(c *gin.Context, symbol string) bool {
	ok, err := h.symbolServable(c.Request.Context(), symbol)
	if err != nil {
		h.Log.Error("api: symbol lookup", "symbol", symbol, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return false
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "unknown symbol"})
		return false
	}
	return true
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

// symbolMetaOrFallback fills in the display name for a symbol from the
// static registry (source.SymbolByCode) when it's a known SIM: symbol,
// falling back to the bare code otherwise (defensive — every symbol this
// API ever serves should be in the static registry in v1).
func symbolMetaOrFallback(symbol string) source.SymbolInfo {
	if meta, ok := source.SymbolByCode(symbol); ok {
		return meta
	}
	return source.SymbolInfo{Symbol: symbol, Name: symbol}
}
