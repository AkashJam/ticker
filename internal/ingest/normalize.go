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
// purely a validation pass — but it's the same seam the Finnhub adapter's
// bars pass through, so it exists as its own step rather than being inlined
// into the producer.
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
	if err := checkBar(tick); err != nil {
		return tick, err
	}
	return tick, nil
}

// checkBar validates the optional Open/High/Low of a coalesced 1-second bar:
// all three or none, High >= Low, and Open and Price (the close) inside them.
// A single-price tick (the sim) leaves all three zero and passes.
func checkBar(tick source.NormalizedTick) error {
	set := 0
	for _, v := range []float64{tick.Open, tick.High, tick.Low} {
		if v != 0 {
			set++
		}
	}
	switch {
	case set == 0:
		return nil
	case set != 3:
		return fmt.Errorf("ingest: normalize: %s: partial bar (open %v, high %v, low %v)", tick.Symbol, tick.Open, tick.High, tick.Low)
	case tick.Open <= 0 || tick.High <= 0 || tick.Low <= 0:
		return fmt.Errorf("ingest: normalize: %s: non-positive bar (open %v, high %v, low %v)", tick.Symbol, tick.Open, tick.High, tick.Low)
	case tick.High < tick.Low:
		return fmt.Errorf("ingest: normalize: %s: bar high %v below low %v", tick.Symbol, tick.High, tick.Low)
	case tick.Open < tick.Low || tick.Open > tick.High || tick.Price < tick.Low || tick.Price > tick.High:
		return fmt.Errorf("ingest: normalize: %s: bar open %v or close %v outside [%v, %v]", tick.Symbol, tick.Open, tick.Price, tick.Low, tick.High)
	}
	return nil
}
