// Package aggregate consumes the ticks:raw Redis Stream via a consumer
// group (portfolio.md §9.1) — not a direct in-process channel from
// internal/ingest — so splitting ingestion and aggregation into separate
// processes later (ADR-003's revisit trigger) needs no pipeline change.
package aggregate

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"sync"
	"time"

	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/store"
)

const (
	ticksStream  = "ticks:raw"
	consumerGrp  = "aggregator"
	readCount    = 50
	readBlock    = 2 * time.Second
	errRetryWait = 1 * time.Second
)

// fixedIntervals are always computed alongside whatever --agg-window
// selects as the finest interval (10s dev / 1m prod) — resolved during
// planning: agg-window controls only the finest interval, so /candles and
// /indicators can serve 5m/15m/1h/1d the same way in both environments.
var fixedIntervals = map[string]time.Duration{
	"5m":  5 * time.Minute,
	"15m": 15 * time.Minute,
	"1h":  time.Hour,
	"1d":  24 * time.Hour,
}

// CandleStore is the one write path processTick/Flush need — narrow enough
// to fake in tests without a real Timescale connection. *store.Timescale
// satisfies it.
type CandleStore interface {
	UpsertCandle(ctx context.Context, c source.Candle) error
}

// StreamConsumer is everything Run needs from Redis — narrowed to an
// interface (rather than depending on *store.Redis directly) so the
// OHLCV/bucketing logic in processTick/Flush can be unit tested with a fake
// that never touches real Redis. *store.Redis satisfies it.
type StreamConsumer interface {
	EnsureConsumerGroup(ctx context.Context, stream, group string) error
	ReadGroup(ctx context.Context, stream, group, consumer string, count int64, block time.Duration) ([]store.StreamMessage, error)
	Ack(ctx context.Context, stream, group string, ids ...string) error
	CacheDelPrefix(ctx context.Context, prefix string) error
	Publish(ctx context.Context, channel string, payload []byte) error
}

type stateKey struct {
	symbol   string
	interval string
}

type openCandle struct {
	bucketStart                    time.Time
	open, high, low, close, volume float64
}

func (oc *openCandle) toCandle(symbol, interval string) source.Candle {
	return source.Candle{
		Time: oc.bucketStart, Symbol: symbol, Interval: interval,
		Open: oc.open, High: oc.high, Low: oc.low, Close: oc.close, Volume: oc.volume,
	}
}

// Aggregator turns the tick stream into OHLCV candles at 5 intervals per
// symbol (the configured finest one, plus the fixed 5m/15m/1h/1d set),
// upserting each to Timescale as it updates and publishing the finest
// interval's updates live over SSE.
type Aggregator struct {
	redis    StreamConsumer
	ts       CandleStore
	finest   string
	finestD  time.Duration
	consumer string
	log      *slog.Logger

	mu    sync.Mutex
	state map[stateKey]*openCandle
}

func New(redis StreamConsumer, ts CandleStore, finestLabel string, finestDur time.Duration, log *slog.Logger) *Aggregator {
	return &Aggregator{
		redis:    redis,
		ts:       ts,
		finest:   finestLabel,
		finestD:  finestDur,
		consumer: consumerName(),
		log:      log,
		state:    make(map[stateKey]*openCandle),
	}
}

func consumerName() string {
	h, err := os.Hostname()
	if err != nil {
		h = "unknown"
	}
	return fmt.Sprintf("%s-%d", h, os.Getpid())
}

func (a *Aggregator) intervals() map[string]time.Duration {
	out := make(map[string]time.Duration, len(fixedIntervals)+1)
	maps.Copy(out, fixedIntervals)
	out[a.finest] = a.finestD // finest wins if it happens to collide with a fixed label
	return out
}

// Run ensures the consumer group exists, then reads and processes entries
// until ctx is canceled. Unacked entries (a failed handleMessage) are left
// for redelivery — at-least-once, tolerated by idempotent candle upserts
// (ADR-004 / Risk Register #4).
func (a *Aggregator) Run(ctx context.Context) error {
	if err := a.redis.EnsureConsumerGroup(ctx, ticksStream, consumerGrp); err != nil {
		return fmt.Errorf("aggregate: %w", err)
	}

	for {
		if ctx.Err() != nil {
			return nil
		}

		messages, err := a.redis.ReadGroup(ctx, ticksStream, consumerGrp, a.consumer, readCount, readBlock)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			a.log.Error("aggregate: read group failed", "error", err)
			time.Sleep(errRetryWait)
			continue
		}

		for _, msg := range messages {
			if err := a.handleMessage(ctx, msg); err != nil {
				a.log.Error("aggregate: handle message failed", "id", msg.ID, "error", err)
				continue // not acked — redelivered on next read
			}
			if err := a.redis.Ack(ctx, ticksStream, consumerGrp, msg.ID); err != nil {
				a.log.Error("aggregate: ack failed", "id", msg.ID, "error", err)
			}
		}
	}
}

func (a *Aggregator) handleMessage(ctx context.Context, msg store.StreamMessage) error {
	if msg.Data == "" {
		return fmt.Errorf("aggregate: message %s missing data field", msg.ID)
	}

	var tick source.NormalizedTick
	if err := json.Unmarshal([]byte(msg.Data), &tick); err != nil {
		return fmt.Errorf("aggregate: unmarshal tick: %w", err)
	}

	return a.processTick(ctx, tick)
}

// processTick updates every tracked interval's in-memory bucket for this
// symbol, upserting each to Timescale as it changes. Upserting on every
// tick (not just on close) keeps the DB's "current" candle always fresh —
// e.g. an SSR fetch of the still-forming bar sees live data, not a stale
// pre-close snapshot.
func (a *Aggregator) processTick(ctx context.Context, tick source.NormalizedTick) error {
	for label, dur := range a.intervals() {
		bucket := tick.Timestamp.Truncate(dur)
		k := stateKey{symbol: tick.Symbol, interval: label}

		a.mu.Lock()
		oc, exists := a.state[k]
		closed := exists && bucket.After(oc.bucketStart)
		if !exists || closed {
			oc = &openCandle{bucketStart: bucket, open: tick.Price, high: tick.Price, low: tick.Price, close: tick.Price, volume: tick.Volume}
			a.state[k] = oc
		} else {
			if tick.Price > oc.high {
				oc.high = tick.Price
			}
			if tick.Price < oc.low {
				oc.low = tick.Price
			}
			oc.close = tick.Price
			oc.volume += tick.Volume
		}
		current := oc.toCandle(tick.Symbol, label)
		a.mu.Unlock()

		if err := a.ts.UpsertCandle(ctx, current); err != nil {
			return fmt.Errorf("aggregate: upsert %s/%s: %w", tick.Symbol, label, err)
		}

		if closed {
			// The just-closed bucket's final state was already durably
			// written on its own last update above (in a prior call) —
			// only the indicator cache for that now-stale window needs
			// invalidating here (§6.1: "On candle close ... → Redis cache").
			prefix := store.IndicatorCachePrefix(tick.Symbol, label)
			if err := a.redis.CacheDelPrefix(ctx, prefix); err != nil {
				a.log.Error("aggregate: cache invalidation failed", "symbol", tick.Symbol, "interval", label, "error", err)
			}
		}

		if label == a.finest {
			a.publishCandle(ctx, current)
		}
	}
	return nil
}

// publishCandle streams the finest interval's live-updating bar over SSE
// (§18's "candlestick" view) — coarser intervals are REST-fetched on tab
// switch rather than also streamed, since only the primary/default view is
// documented as needing live updates.
func (a *Aggregator) publishCandle(ctx context.Context, c source.Candle) {
	payload, err := json.Marshal(c)
	if err != nil {
		a.log.Error("aggregate: marshal candle", "error", err)
		return
	}
	envelope, err := json.Marshal(source.PubSubMessage{Type: "candle", Payload: payload})
	if err != nil {
		a.log.Error("aggregate: marshal candle envelope", "error", err)
		return
	}
	if err := a.redis.Publish(ctx, store.QuoteChannel(c.Symbol), envelope); err != nil {
		a.log.Error("aggregate: publish candle failed", "symbol", c.Symbol, "error", err)
	}
}

// Flush upserts every currently-open in-memory candle — called from the
// shutdown path (portfolio.md §15: "the aggregator flushes in-flight
// candles before exit"). Each tick's processTick call already upserts
// synchronously, so this is a safety net proving the final state is
// persisted rather than a load-bearing batch write.
func (a *Aggregator) Flush(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for k, oc := range a.state {
		if err := a.ts.UpsertCandle(ctx, oc.toCandle(k.symbol, k.interval)); err != nil {
			return fmt.Errorf("aggregate: flush %s/%s: %w", k.symbol, k.interval, err)
		}
	}
	return nil
}
