// Package aggregate consumes the ticks:raw Redis Stream via a consumer
// group (portfolio.md §9.1) — not a direct in-process channel from
// internal/ingest — so splitting ingestion and aggregation into separate
// processes later (ADR-003's revisit trigger) needs no pipeline change.
package aggregate

import (
	"context"
	"encoding/json"
	"errors"
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

	// reclaimMinIdle is how long an entry must sit unacked in another
	// consumer's PEL before this one claims it — long enough that a
	// genuinely in-flight handleMessage call isn't stolen out from under it.
	reclaimMinIdle = 60 * time.Second
	// reclaimEvery interleaves a reclaim pass roughly once a minute: ReadGroup
	// already blocks up to readBlock (2s) per loop iteration, so ~30
	// iterations ≈ 1 minute of wall time in the steady state.
	reclaimEvery = 30

	// sweepEvery is how often the timer looks for buckets whose window has
	// ended. A bucket otherwise closes only when the next tick for that
	// symbol arrives, so the last bars of a session (and any quiet symbol)
	// would sit unwritten until the next one.
	sweepEvery = 5 * time.Second
	// sweepGrace is how long past its end a bucket stays open, so a tick that
	// arrives a little late (aggregator lag behind the stream) still lands in
	// its own bucket instead of being dropped as late.
	sweepGrace = 5 * time.Second
	// maxPending bounds the closed candles kept for retry while the store is
	// failing; beyond it the oldest are dropped and logged.
	maxPending = 10_000

	// dailyInterval is the bar the daily /quote sweep owns for real symbols
	// (portfolio.md §15 Phase 8), so the aggregator builds it only for
	// simulated ticks.
	dailyInterval = "1d"
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

// CandleStore is the write path processTick/Flush/handleMessage need —
// narrow enough to fake in tests without a real Timescale connection.
// *store.Timescale satisfies it.
type CandleStore interface {
	UpsertCandle(ctx context.Context, c source.Candle) error
	// InsertTick appends the raw tick keyed by (time, streamID) and reports
	// whether it was new — false means this stream entry was already
	// processed (content/REVIEW.md's central design: the insert is the
	// dedupe gate for at-least-once redelivery).
	InsertTick(ctx context.Context, tick source.NormalizedTick, streamID string) (inserted bool, err error)
}

// StreamConsumer is everything Run needs from Redis — narrowed to an
// interface (rather than depending on *store.Redis directly) so the
// OHLCV/bucketing logic in processTick/Flush can be unit tested with a fake
// that never touches real Redis. *store.Redis satisfies it.
type StreamConsumer interface {
	EnsureConsumerGroup(ctx context.Context, stream, group string) error
	ReadGroup(ctx context.Context, stream, group, consumer string, count int64, block time.Duration) ([]store.StreamMessage, error)
	// AutoClaim reclaims entries idle at least minIdle from other consumers'
	// PELs — makes redelivery real for a crashed/stalled consumer's pending
	// entries instead of leaving them stuck forever (content/REVIEW.md #2/#4).
	AutoClaim(ctx context.Context, stream, group, consumer string, minIdle time.Duration, start string, count int64) (msgs []store.StreamMessage, next string, err error)
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
	volume := oc.volume
	return source.Candle{
		Time: oc.bucketStart, Symbol: symbol, Interval: interval,
		Open: oc.open, High: oc.high, Low: oc.low, Close: oc.close, Volume: &volume,
	}
}

// Aggregator turns the tick stream into OHLCV candles at up to 5 intervals
// per symbol (the configured finest one, plus the fixed 5m/15m/1h set, and 1d
// for simulated symbols only). A candle is written to Timescale once, when its
// bucket closes: on the next tick, from the timer sweep once the bucket's
// window has passed, or at shutdown. The finest interval's updates are
// published live over SSE on every tick, independent of any write.
type Aggregator struct {
	redis    StreamConsumer
	ts       CandleStore
	finest   string
	finestD  time.Duration
	consumer string
	log      *slog.Logger

	now func() time.Time

	mu    sync.Mutex
	state map[stateKey]*openCandle
	// closedThrough is the start of the newest bucket the timer swept for a
	// key, so a tick arriving after the grace period cannot reopen it.
	closedThrough map[stateKey]time.Time
	// pending holds closed candles whose write failed, retried by the timer
	// and at shutdown. Their in-memory state is already gone.
	pending []source.Candle
}

// New wires an Aggregator. consumer names this instance in the Redis
// consumer group — pass "" to fall back to the single-aggregator default
// (hostname-pid). A fixed non-empty name matters once ADR-003's revisit
// trigger fires and a second aggregator replica appears: hostname-pid
// orphans pending entries on every deploy (container hostnames change), but
// AutoClaim (Run) sweeps up whatever a prior name left pending regardless.
func New(redis StreamConsumer, ts CandleStore, finestLabel string, finestDur time.Duration, consumer string, log *slog.Logger) *Aggregator {
	if consumer == "" {
		consumer = defaultConsumerName()
	}
	return &Aggregator{
		redis:    redis,
		ts:       ts,
		finest:   finestLabel,
		finestD:  finestDur,
		consumer: consumer,
		log:      log,
		now:      time.Now,

		state:         make(map[stateKey]*openCandle),
		closedThrough: make(map[stateKey]time.Time),
	}
}

func defaultConsumerName() string {
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
// pending and picked back up either by this consumer's own next ReadGroup
// (still on its PEL) or, if this consumer stalls or dies first, by the
// periodic reclaim pass below claiming them from another consumer —
// at-least-once delivery, made safe by the stream-ID dedupe gate in
// handleMessage rather than by upsert idempotency alone (content/REVIEW.md).
func (a *Aggregator) Run(ctx context.Context) error {
	if err := a.redis.EnsureConsumerGroup(ctx, ticksStream, consumerGrp); err != nil {
		return fmt.Errorf("aggregate: %w", err)
	}

	swept := make(chan struct{})
	go func() {
		defer close(swept)
		a.sweepLoop(ctx)
	}()
	defer func() { <-swept }()

	iterations := 0
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

		a.handleAndAck(ctx, messages)

		iterations++
		if iterations%reclaimEvery == 0 {
			a.reclaim(ctx)
		}
	}
}

// reclaim claims entries idle at least reclaimMinIdle from other consumers'
// PELs — the mechanism that makes redelivery real rather than aspirational
// (content/REVIEW.md #2/#4): a consumer that crashed or hung mid-handleMessage
// leaves its pending entries stranded until something else claims them.
// Claimed messages go through the same handleMessage path as freshly read
// ones, so they inherit the dedupe gate for free.
func (a *Aggregator) reclaim(ctx context.Context) {
	messages, _, err := a.redis.AutoClaim(ctx, ticksStream, consumerGrp, a.consumer, reclaimMinIdle, "0", readCount)
	if err != nil {
		a.log.Error("aggregate: reclaim failed", "error", err)
		return
	}
	a.handleAndAck(ctx, messages)
}

func (a *Aggregator) handleAndAck(ctx context.Context, messages []store.StreamMessage) {
	for _, msg := range messages {
		if err := a.handleMessage(ctx, msg); err != nil {
			a.log.Error("aggregate: handle message failed", "id", msg.ID, "error", err)
			continue // not acked — redelivered on next read or the next reclaim pass
		}
		if err := a.redis.Ack(ctx, ticksStream, consumerGrp, msg.ID); err != nil {
			a.log.Error("aggregate: ack failed", "id", msg.ID, "error", err)
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

	// Insert-first, deliberately: this is the dedupe gate (content/REVIEW.md's
	// central design), not just ADR-004's raw tape. A zero-row insert means
	// this stream entry (redelivered or reclaimed from a dead consumer) was
	// already processed — ack and skip, no accumulation. The reverse order
	// (accumulate, then record) would double-count volume on any crash
	// between the two.
	inserted, err := a.ts.InsertTick(ctx, tick, msg.ID)
	if err != nil {
		return fmt.Errorf("aggregate: insert tick %s: %w", msg.ID, err)
	}
	if !inserted {
		return nil
	}

	return a.processTick(ctx, tick)
}

// applied is what one tick changed: the candles whose bucket it closed
// (final state, ready to write) and the finest interval's current state
// for the live SSE publish.
type applied struct {
	closed []source.Candle
	finest *source.Candle
}

// upsertMaxAttempts bounds one candle write's retry — narrows (doesn't
// close) the residual documented in content/REVIEW.md. A write that still
// fails is parked in pending and retried by the timer sweep.
const upsertMaxAttempts = 3

// processTick applies a tick to every tracked interval's in-memory bucket,
// writes whichever candles that tick closed, and publishes the finest
// interval's current bar. Open buckets are not written: a candle is written
// once, on close (portfolio.md §15 Phase 13), which is what takes the DB from
// one write per interval per tick to roughly the tick insert alone.
func (a *Aggregator) processTick(ctx context.Context, tick source.NormalizedTick) error {
	res := a.applyTick(tick)

	a.writeClosed(ctx, res.closed)

	if res.finest != nil {
		a.publishCandle(ctx, *res.finest)
	}
	return nil
}

// applyTick updates every tracked interval's in-memory bucket for this
// symbol under a single lock. A bucket closes when a tick lands in a later
// one; its final state is returned for writing. A tick for a bucket older
// than the open one, or one the timer has already swept, is left out of that
// interval (its raw tick is still stored): merging it into a newer bucket, or
// reopening a closed one, would corrupt a bar.
func (a *Aggregator) applyTick(tick source.NormalizedTick) applied {
	var out applied

	a.mu.Lock()
	defer a.mu.Unlock()
	for label, dur := range a.intervals() {
		if label == dailyInterval && !tick.Simulated {
			continue
		}
		bucket := tick.Timestamp.Truncate(dur)
		k := stateKey{symbol: tick.Symbol, interval: label}

		oc, exists := a.state[k]
		if exists && bucket.Before(oc.bucketStart) {
			continue
		}
		if !exists {
			if through, ok := a.closedThrough[k]; ok && !bucket.After(through) {
				continue
			}
		}

		if exists && bucket.After(oc.bucketStart) {
			out.closed = append(out.closed, oc.toCandle(tick.Symbol, label))
			exists = false
		}
		if !exists {
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
		if label == a.finest {
			c := oc.toCandle(tick.Symbol, label)
			out.finest = &c
		}
	}
	return out
}

// writeClosed persists candles whose bucket has closed, then invalidates each
// one's indicator cache (§6.1: "On candle close ... → Redis cache"). A write
// that fails every attempt is parked in pending for the timer sweep rather
// than dropped, because the bucket's in-memory state is already gone.
func (a *Aggregator) writeClosed(ctx context.Context, candles []source.Candle) {
	for _, c := range candles {
		if err := a.upsertWithRetry(ctx, c); err != nil {
			a.park(c, err)
			continue
		}
		prefix := store.IndicatorCachePrefix(c.Symbol, c.Interval)
		if err := a.redis.CacheDelPrefix(ctx, prefix); err != nil {
			a.log.Error("aggregate: cache invalidation failed", "symbol", c.Symbol, "interval", c.Interval, "error", err)
		}
	}
}

func (a *Aggregator) park(c source.Candle, cause error) {
	a.log.Error("aggregate: candle write failed, will retry", "symbol", c.Symbol, "interval", c.Interval, "time", c.Time, "error", cause)
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.pending) >= maxPending {
		a.log.Error("aggregate: pending candles full, dropping the oldest", "max", maxPending)
		a.pending = a.pending[1:]
	}
	a.pending = append(a.pending, c)
}

// sweepLoop runs sweepExpired on a timer until ctx is canceled.
func (a *Aggregator) sweepLoop(ctx context.Context) {
	t := time.NewTicker(sweepEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.sweepExpired(ctx, a.now())
		}
	}
}

// sweepExpired closes every bucket whose window ended at least sweepGrace
// before now, without waiting for another tick, and retries candles whose
// earlier write failed. Writing happens outside the lock.
func (a *Aggregator) sweepExpired(ctx context.Context, now time.Time) {
	intervals := a.intervals()

	a.mu.Lock()
	var due []source.Candle
	for k, oc := range a.state {
		if oc.bucketStart.Add(intervals[k.interval]).Add(sweepGrace).After(now) {
			continue
		}
		due = append(due, oc.toCandle(k.symbol, k.interval))
		a.closedThrough[k] = oc.bucketStart
		delete(a.state, k)
	}
	retry := a.pending
	a.pending = nil
	a.mu.Unlock()

	a.writeClosed(ctx, append(retry, due...))
}

// upsertWithRetry retries a single candle's UpsertCandle up to
// upsertMaxAttempts times before giving up.
func (a *Aggregator) upsertWithRetry(ctx context.Context, c source.Candle) error {
	var err error
	for attempt := 1; attempt <= upsertMaxAttempts; attempt++ {
		if err = a.ts.UpsertCandle(ctx, c); err == nil {
			return nil
		}
		if attempt < upsertMaxAttempts {
			a.log.Warn("aggregate: upsert failed, retrying", "symbol", c.Symbol, "interval", c.Interval, "attempt", attempt, "error", err)
		}
	}
	return err
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

// Flush writes every currently-open in-memory candle and any candle still
// waiting on a retry — called from the shutdown path (portfolio.md §15: "the
// aggregator flushes in-flight candles before exit"). Since open buckets are
// no longer written per tick, this is the only thing that persists the bars
// in progress at a graceful stop.
func (a *Aggregator) Flush(ctx context.Context) error {
	a.mu.Lock()
	all := a.pending
	a.pending = nil
	for k, oc := range a.state {
		all = append(all, oc.toCandle(k.symbol, k.interval))
	}
	a.mu.Unlock()

	var errs []error
	for _, c := range all {
		if err := a.ts.UpsertCandle(ctx, c); err != nil {
			errs = append(errs, fmt.Errorf("aggregate: flush %s/%s: %w", c.Symbol, c.Interval, err))
		}
	}
	return errors.Join(errs...)
}
