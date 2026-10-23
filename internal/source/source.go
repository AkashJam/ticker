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
// make it the wire format for the Redis Stream entry and the quote cache,
// which are internal to this service. The SSE `quote` event is the public
// face of a tick and is a separate type, QuoteEvent, so what only the
// pipeline needs (Simulated) never reaches a reader.
type NormalizedTick struct {
	Symbol string `json:"symbol"`
	// Price is the tick's price; for a coalesced bar (Finnhub) it is the
	// close, and Open/High/Low carry the rest of the bar.
	Price     float64   `json:"price"`
	Volume    float64   `json:"volume"`
	Timestamp time.Time `json:"time"`
	AssetType AssetType `json:"assetType"`
	// Open, High and Low are set only when the tick is a 1-second bar built
	// from many trades (ADR-004's amendment at Phase 13), so candles built
	// from bars keep the intra-second extremes. Zero means absent — the sim's
	// single-price ticks leave them empty and the aggregator falls back to
	// Price.
	Open float64 `json:"open,omitempty"`
	High float64 `json:"high,omitempty"`
	Low  float64 `json:"low,omitempty"`
	// Simulated is true when the tick was produced by the sim source. It is
	// internal to dev and CI (portfolio.md ADR-006): the aggregator uses it to
	// give SIM: symbols their own daily bar. It is not stored (the ticks column
	// was dropped by migration 000010) and not part of QuoteEvent, so no row,
	// API response or SSE event carries it.
	Simulated bool `json:"simulated"`
}

// QuoteEvent is the SSE `quote` payload (§8): a tick as a reader sees it.
// Time is the trade time, which is the value's vintage (ADR-006).
type QuoteEvent struct {
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Volume    float64   `json:"volume"`
	Timestamp time.Time `json:"time"`
	AssetType AssetType `json:"assetType"`
	Open      float64   `json:"open,omitempty"`
	High      float64   `json:"high,omitempty"`
	Low       float64   `json:"low,omitempty"`
}

// Public drops what only the pipeline needs.
func (t NormalizedTick) Public() QuoteEvent {
	return QuoteEvent{
		Symbol: t.Symbol, Price: t.Price, Volume: t.Volume, Timestamp: t.Timestamp,
		AssetType: t.AssetType, Open: t.Open, High: t.High, Low: t.Low,
	}
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
	// EmittedAt is when a quote entered the pipeline (the producer received
	// the tick), the start of the hub's latency span. It is the envelope's, not
	// the QuoteEvent's: a Finnhub tick is stamped with its bar's first second,
	// so timing from that stamp would measure the 1 s bar and its grace, not
	// the pipeline. Zero on candles and on an older publisher's quotes.
	EmittedAt time.Time `json:"emittedAt,omitzero"`
}

// MarketSource is the seam every feed implementation satisfies — Sim and
// Finnhub (finnhub.go) are drop-in implementations behind the same interface,
// never a fork of the pipeline.
type MarketSource interface {
	// Subscribe streams normalized ticks for the given symbols until ctx is
	// canceled, at which point the returned channel is closed.
	Subscribe(ctx context.Context, symbols []string) (<-chan NormalizedTick, error)

	// Backfill fetches historical candles for [from, to) — used to fill
	// gaps after a reconnect. The sim source (v1's only source) has no
	// "outage" to backfill from and returns an empty slice.
	Backfill(ctx context.Context, symbol string, from, to time.Time) ([]Candle, error)

	// Name identifies the source ("sim" | "finnhub") — surfaced end-to-end
	// in logs and metrics. It is not shown to readers: the API carries a
	// value's vintage (`asOf`), not its source (ADR-006).
	Name() string
}
