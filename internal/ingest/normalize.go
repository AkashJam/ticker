// Package ingest is the leader-elected pipeline stage that takes ticks from
// a source.MarketSource and hands them to the aggregator via a Redis
// Stream (portfolio.md §6.1, §9.1) — never a direct in-process channel, so
// splitting ingestion and aggregation into separate processes later (the
// ADR-003 revisit trigger) needs no pipeline change, only a deployment one.
package ingest

import (
	"fmt"
	"time"

	"github.com/AkashJam/ticker/internal/source"
)

// maxClockSkew bounds how far in the future a tick's timestamp may be
// before it's rejected — a defensive check against a misbehaving source,
// not something the sim source (which always stamps time.Now()) should
// ever trip.
const maxClockSkew = 5 * time.Second

// Normalize validates a tick at the pipeline boundary. In v1 the sim
// source already emits the NormalizedTick shape directly, so this is
// purely a validation pass — but it's the same seam a future Finnhub
// adapter's raw-format conversion would plug into (§16.1), so it exists
// now rather than being inlined into the producer.
func Normalize(tick source.NormalizedTick) (source.NormalizedTick, error) {
	if tick.Symbol == "" {
		return tick, fmt.Errorf("ingest: normalize: empty symbol")
	}
	if tick.Price <= 0 {
		return tick, fmt.Errorf("ingest: normalize: %s: non-positive price %v", tick.Symbol, tick.Price)
	}
	if tick.Volume < 0 {
		return tick, fmt.Errorf("ingest: normalize: %s: negative volume %v", tick.Symbol, tick.Volume)
	}
	if tick.Timestamp.IsZero() {
		return tick, fmt.Errorf("ingest: normalize: %s: zero timestamp", tick.Symbol)
	}
	if tick.Timestamp.After(time.Now().Add(maxClockSkew)) {
		return tick, fmt.Errorf("ingest: normalize: %s: timestamp too far in the future: %s", tick.Symbol, tick.Timestamp)
	}
	return tick, nil
}
