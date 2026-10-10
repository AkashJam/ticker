// Package source defines the MarketSource abstraction (portfolio.md §6.2) —
// the seam that lets v1's simulated feed and a future live feed (§16.1,
// source/finnhub.go) plug into the same ingestion pipeline unmodified.
package source

import (
	"context"
	"encoding/json"
	"time"
)

// AssetType classifies a NormalizedTick's instrument.
type AssetType string

const (
	AssetTypeStock  AssetType = "stock"
	AssetTypeCrypto AssetType = "crypto"
	AssetTypeForex  AssetType = "forex"
	AssetTypeETF    AssetType = "etf"
)

// NormalizedTick is the common shape every MarketSource emits, regardless of
// upstream format — this is what the ingestion pipeline consumes. JSON tags
// double this struct as the wire format for the Redis Stream entry and the
// SSE `quote` event (§8) — both are internal-to-this-service or
// internal-to-this-repo contracts, not a versioned public API, so one type
// serving both is intentional rather than premature reuse.
type NormalizedTick struct {
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Volume    float64   `json:"volume"`
	Timestamp time.Time `json:"time"`
	AssetType AssetType `json:"assetType"`
	// Simulated is true when the tick was produced by the sim source, never
	// a live feed. Carried end-to-end through storage and the API so the
	// UI can flag it — it must never be able to masquerade as real market
	// data (ADR-005).
	Simulated bool `json:"simulated"`
}

// Candle is one OHLCV bar for a symbol at a given interval, produced by the
// aggregator (internal/aggregate) from a stream of NormalizedTicks and
// returned by Backfill for gap-filling after a reconnect. JSON tags match
// §8's flat candle shape, reused as-is for both /candles responses and the
// SSE `candle` event payload.
type Candle struct {
	Time time.Time `json:"time"`
	// Interval is one of "1m", "5m", "15m", "1h", "1d" (or whatever finer
	// interval --agg-window selects in dev — see internal/aggregate).
	Symbol   string  `json:"symbol"`
	Interval string  `json:"interval"`
	Open     float64 `json:"open"`
	High     float64 `json:"high"`
	Low      float64 `json:"low"`
	Close    float64 `json:"close"`
	// Volume is nil when the bar has no measured volume — the daily sweep's
	// /quote carries none (portfolio.md §15 Phase 8), and absent is honest
	// where a stored 0 would read as a real zero-volume day. Omitted from
	// JSON when nil.
	Volume *float64 `json:"volume,omitempty"`
}

// PubSubMessage envelopes a quote or candle update published to
// quotes:{symbol} (§6.3) — internal/sse's Hub unwraps Type to pick the SSE
// event name (§8's `event: quote` / `event: candle`), keeping one Redis
// channel per symbol instead of two.
type PubSubMessage struct {
	Type    string          `json:"type"` // "quote" | "candle"
	Payload json.RawMessage `json:"payload"`
}

// MarketSource is the seam every feed implementation satisfies — v1 ships
// only Sim (this package); Finnhub (§16.1) is a drop-in second
// implementation behind the same interface, never a fork of the pipeline.
type MarketSource interface {
	// Subscribe streams normalized ticks for the given symbols until ctx is
	// canceled, at which point the returned channel is closed.
	Subscribe(ctx context.Context, symbols []string) (<-chan NormalizedTick, error)

	// Backfill fetches historical candles for [from, to) — used to fill
	// gaps after a reconnect. The sim source (v1's only source) has no
	// "outage" to backfill from and returns an empty slice.
	Backfill(ctx context.Context, symbol string, from, to time.Time) ([]Candle, error)

	// Name identifies the source ("sim" | "finnhub") — surfaced end-to-end
	// via NormalizedTick.Simulated / the API's `simulated` field so the UI
	// can badge it (ADR-005).
	Name() string
}
