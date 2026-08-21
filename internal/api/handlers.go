// Package api implements the Gin REST surface (portfolio.md §8) — internal
// only, no public route (Option B): reached exclusively over the Docker
// network by the Next.js server, never directly by a browser.
package api

import (
	"log/slog"
	"math"

	"github.com/AkashJam/ticker/internal/indicators"
	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/store"
)

const indicatorHistoryLimit = 200

// Handlers holds every dependency the route handlers need. A plain struct
// with method-per-endpoint (handlers_*.go) rather than per-endpoint free
// functions with closures — keeps the dependency list visible in one place.
type Handlers struct {
	Meta  *store.Meta
	TS    *store.Timescale
	Redis *store.Redis
	Cache *indicators.Cache
	Ready func() error // aggregate readiness check (§8: GET /ready) — see server.go
	Log   *slog.Logger
}

func NewHandlers(meta *store.Meta, ts *store.Timescale, redis *store.Redis, cache *indicators.Cache, ready func() error, log *slog.Logger) *Handlers {
	return &Handlers{Meta: meta, TS: ts, Redis: redis, Cache: cache, Ready: ready, Log: log}
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
