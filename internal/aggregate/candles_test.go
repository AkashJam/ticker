package aggregate

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/store"
)

// candleKey mirrors the store's real (symbol, interval, time) primary key,
// so this fake's overwrite-on-repeat-key semantics match Timescale's
// ON CONFLICT DO UPDATE exactly.
type candleKey struct {
	symbol, interval string
	time             time.Time
}

// tickKey mirrors the store's real (time, stream_id) unique constraint that
// makes InsertTick double as the dedupe gate.
type tickKey struct {
	time     time.Time
	streamID string
}

type fakeCandleStore struct {
	mu     sync.Mutex
	latest map[candleKey]source.Candle
	calls  int
	ticks  map[tickKey]bool
	// failures is how many UpsertCandle calls fail before writes succeed.
	failures int
}

func newFakeCandleStore() *fakeCandleStore {
	return &fakeCandleStore{latest: make(map[candleKey]source.Candle), ticks: make(map[tickKey]bool)}
}

func (f *fakeCandleStore) UpsertCandle(_ context.Context, c source.Candle) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failures > 0 {
		f.failures--
		return errors.New("fake store: write failed")
	}
	f.latest[candleKey{c.Symbol, c.Interval, c.Time}] = c
	return nil
}

// InsertTick mirrors Timescale's ON CONFLICT (time, stream_id) DO NOTHING —
// inserted is false on a repeat (time, streamID) pair.
func (f *fakeCandleStore) InsertTick(_ context.Context, tick source.NormalizedTick, streamID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := tickKey{time: tick.Timestamp, streamID: streamID}
	if f.ticks[k] {
		return false, nil
	}
	f.ticks[k] = true
	return true, nil
}

func (f *fakeCandleStore) get(symbol, interval string, t time.Time) (source.Candle, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.latest[candleKey{symbol, interval, t}]
	return c, ok
}

// fakeRedis satisfies StreamConsumer without touching real Redis — only
// CacheDelPrefix/Publish are exercised by processTick/Flush; the
// Stream-consumption methods exist only so New()'s constructor is
// satisfied (Run itself isn't unit tested — it needs a real Stream).
type fakeRedis struct {
	mu          sync.Mutex
	invalidated []string
	published   [][]byte
}

func (f *fakeRedis) EnsureConsumerGroup(_ context.Context, _, _ string) error {
	return nil
}

func (f *fakeRedis) ReadGroup(_ context.Context, _, _, _ string, _ int64, _ time.Duration) ([]store.StreamMessage, error) {
	return nil, nil
}

func (f *fakeRedis) AutoClaim(_ context.Context, _, _, _ string, _ time.Duration, _ string, _ int64) ([]store.StreamMessage, string, error) {
	return nil, "0", nil
}

func (f *fakeRedis) Ack(_ context.Context, _, _ string, _ ...string) error {
	return nil
}

func (f *fakeRedis) CacheDelPrefix(_ context.Context, prefix string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.invalidated = append(f.invalidated, prefix)
	return nil
}

func (f *fakeRedis) Publish(_ context.Context, _ string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, payload)
	return nil
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func newTestAggregator(redis StreamConsumer, ts CandleStore) *Aggregator {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(redis, ts, "1m", time.Minute, "test-consumer", log)
}

func TestAggregator_OHLCVCorrectness(t *testing.T) {
	ts := newFakeCandleStore()
	agg := newTestAggregator(&fakeRedis{}, ts)
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC) // exact minute boundary
	ticks := []source.NormalizedTick{
		{Symbol: "SIM:TEST", Price: 100, Volume: 10, Timestamp: base.Add(1 * time.Second)},
		{Symbol: "SIM:TEST", Price: 105, Volume: 20, Timestamp: base.Add(10 * time.Second)},
		{Symbol: "SIM:TEST", Price: 98, Volume: 15, Timestamp: base.Add(20 * time.Second)},
	}
	for _, tick := range ticks {
		must(t, agg.processTick(ctx, tick))
	}
	if ts.calls != 0 {
		t.Fatalf("open bucket was written %d times, want 0 until it closes", ts.calls)
	}

	// A tick in the next minute closes the bucket and writes it once.
	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:TEST", Price: 1, Volume: 1, Timestamp: base.Add(time.Minute)}))

	got, ok := ts.get("SIM:TEST", "1m", base)
	if !ok {
		t.Fatalf("expected the closed 1m candle to be written")
	}
	want := source.Candle{Time: base, Symbol: "SIM:TEST", Interval: "1m", Open: 100, High: 105, Low: 98, Close: 98, Volume: vol(45)}
	if !sameCandle(got, want) {
		t.Errorf("candle = %+v, want %+v", got, want)
	}
	if ts.calls != 1 {
		t.Errorf("UpsertCandle calls = %d, want 1 (only the 1m bucket closed)", ts.calls)
	}
}

func TestAggregator_CandleClose(t *testing.T) {
	ts := newFakeCandleStore()
	redis := &fakeRedis{}
	agg := newTestAggregator(redis, ts)
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:TEST", Price: 100, Volume: 5, Timestamp: base}))
	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:TEST", Price: 110, Volume: 5, Timestamp: base.Add(30 * time.Second)}))

	next := base.Add(time.Minute) // crosses into the next 1m bucket
	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:TEST", Price: 90, Volume: 7, Timestamp: next}))

	closed, ok := ts.get("SIM:TEST", "1m", base)
	if !ok {
		t.Fatalf("expected the closed bucket's candle in the store")
	}
	wantClosed := source.Candle{Time: base, Symbol: "SIM:TEST", Interval: "1m", Open: 100, High: 110, Low: 100, Close: 110, Volume: vol(10)}
	if !sameCandle(closed, wantClosed) {
		t.Errorf("closed candle = %+v, want %+v", closed, wantClosed)
	}

	if _, ok := ts.get("SIM:TEST", "1m", next.Truncate(time.Minute)); ok {
		t.Errorf("the new, still-open bucket must not be written until it closes")
	}

	redis.mu.Lock()
	defer redis.mu.Unlock()
	wantPrefix := store.IndicatorCachePrefix("SIM:TEST", "1m")
	found := false
	for _, p := range redis.invalidated {
		if p == wantPrefix {
			found = true
		}
	}
	if !found {
		t.Errorf("expected cache invalidation for prefix %q, got %v", wantPrefix, redis.invalidated)
	}
}

func TestAggregator_UpsertIdempotency(t *testing.T) {
	// SQL-level idempotency only: candle upsert is idempotent by
	// (symbol, interval, time) — writing the same candle state repeatedly
	// (as a retry or a flush after a close would) must
	// never produce more than one stored row for that key. Redelivery safety
	// is a separate claim, covered by TestAggregator_DuplicateTickIsSkipped.
	ts := newFakeCandleStore()
	ctx := context.Background()
	c := source.Candle{Symbol: "SIM:TEST", Interval: "1m", Time: time.Unix(0, 0).UTC(), Open: 100, High: 105, Low: 98, Close: 102, Volume: vol(40)}

	must(t, ts.UpsertCandle(ctx, c))
	must(t, ts.UpsertCandle(ctx, c)) // repeat write of the identical state

	if ts.calls != 2 {
		t.Fatalf("expected 2 UpsertCandle calls, got %d", ts.calls)
	}
	if len(ts.latest) != 1 {
		t.Errorf("expected exactly one stored row for the (symbol,interval,time) key, got %d", len(ts.latest))
	}
	got, ok := ts.get(c.Symbol, c.Interval, c.Time)
	if !ok || !sameCandle(got, c) {
		t.Errorf("stored candle = %+v, want %+v", got, c)
	}
}

// TestAggregator_DuplicateTickIsSkipped drives handleMessage twice with the
// same stream ID (as a redelivered/reclaimed Stream entry would) and asserts
// the tick's effect — volume — is only counted once. This must fail before
// the stream_id insert-as-dedupe gate lands (content/REVIEW.md's central
// design) and pass after, or it isn't proving anything.
func TestAggregator_DuplicateTickIsSkipped(t *testing.T) {
	ts := newFakeCandleStore()
	agg := newTestAggregator(&fakeRedis{}, ts)
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tick := source.NormalizedTick{Symbol: "SIM:TEST", Price: 100, Volume: 10, Timestamp: base}
	payload, err := json.Marshal(tick)
	if err != nil {
		t.Fatalf("marshal tick: %v", err)
	}
	msg := store.StreamMessage{ID: "1-0", Data: string(payload)}

	must(t, agg.handleMessage(ctx, msg))
	must(t, agg.handleMessage(ctx, msg)) // redelivery of the same stream entry

	must(t, agg.Flush(ctx)) // open buckets are only written on close or flush
	got, ok := ts.get("SIM:TEST", "1m", base)
	if !ok {
		t.Fatalf("expected a flushed 1m candle")
	}
	if got.Volume == nil || *got.Volume != tick.Volume {
		t.Errorf("volume = %v, want %v (duplicate delivery must not double-count)", fmtVol(got.Volume), tick.Volume)
	}
}

func TestAggregator_Flush(t *testing.T) {
	ts := newFakeCandleStore()
	agg := newTestAggregator(&fakeRedis{}, ts)
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:A", Price: 50, Volume: 1, Timestamp: base}))
	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:B", Price: 60, Volume: 1, Timestamp: base}))

	if ts.calls != 0 {
		t.Fatalf("expected no writes before Flush, got %d", ts.calls)
	}
	must(t, agg.Flush(ctx))
	if ts.calls == 0 {
		t.Errorf("expected Flush to write the open candles")
	}

	for _, sym := range []string{"SIM:A", "SIM:B"} {
		if _, ok := ts.get(sym, "1m", base); !ok {
			t.Errorf("Flush: expected a persisted candle for %s/1m", sym)
		}
	}
}

func TestAggregator_SweepClosesExpiredBuckets(t *testing.T) {
	ts := newFakeCandleStore()
	redis := &fakeRedis{}
	agg := newTestAggregator(redis, ts)
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:TEST", Price: 100, Volume: 5, Timestamp: base.Add(10 * time.Second)}))

	// The 1m bucket ends at 12:01:00; it stays open through the grace period.
	agg.sweepExpired(ctx, base.Add(time.Minute+sweepGrace-time.Second))
	if ts.calls != 0 {
		t.Fatalf("swept before end+grace: %d writes", ts.calls)
	}

	agg.sweepExpired(ctx, base.Add(time.Minute+sweepGrace))
	got, ok := ts.get("SIM:TEST", "1m", base)
	if !ok {
		t.Fatalf("expected the expired 1m bucket to be written with no further tick")
	}
	if got.Open != 100 || got.Close != 100 {
		t.Errorf("swept candle = %+v", got)
	}
	// 5m, 15m and 1h are still inside their windows.
	for _, iv := range []string{"5m", "15m", "1h"} {
		if _, ok := ts.get("SIM:TEST", iv, base); ok {
			t.Errorf("%s bucket written before it ended", iv)
		}
	}

	// A second sweep must not rewrite it, and the cache is invalidated once.
	calls := ts.calls
	agg.sweepExpired(ctx, base.Add(2*time.Minute))
	if ts.calls != calls {
		t.Errorf("an already-swept bucket was written again")
	}
	redis.mu.Lock()
	defer redis.mu.Unlock()
	if len(redis.invalidated) != 1 {
		t.Errorf("cache invalidations = %v, want one for the swept 1m bucket", redis.invalidated)
	}
}

func TestAggregator_LateTickDoesNotReopenSweptBucket(t *testing.T) {
	ts := newFakeCandleStore()
	agg := newTestAggregator(&fakeRedis{}, ts)
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:TEST", Price: 100, Volume: 5, Timestamp: base}))
	agg.sweepExpired(ctx, base.Add(time.Minute+sweepGrace))

	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:TEST", Price: 50, Volume: 99, Timestamp: base.Add(30 * time.Second)}))
	must(t, agg.Flush(ctx))

	got, _ := ts.get("SIM:TEST", "1m", base)
	if got.Low != 100 || (got.Volume != nil && *got.Volume != 5) {
		t.Errorf("late tick altered a closed bar: %+v", got)
	}
}

func TestAggregator_FailedWriteIsRetriedBySweep(t *testing.T) {
	ts := newFakeCandleStore()
	agg := newTestAggregator(&fakeRedis{}, ts)
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:TEST", Price: 100, Volume: 5, Timestamp: base}))

	ts.failures = upsertMaxAttempts // every attempt for the closing write fails
	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:TEST", Price: 90, Volume: 1, Timestamp: base.Add(time.Minute)}))
	if _, ok := ts.get("SIM:TEST", "1m", base); ok {
		t.Fatalf("write should have failed")
	}

	agg.sweepExpired(ctx, base.Add(time.Minute)) // store healthy again; nothing newly expired
	got, ok := ts.get("SIM:TEST", "1m", base)
	if !ok || got.Close != 100 {
		t.Errorf("failed close write was lost: got %+v, found=%v", got, ok)
	}
}

func TestAggregator_FlushWritesPending(t *testing.T) {
	ts := newFakeCandleStore()
	agg := newTestAggregator(&fakeRedis{}, ts)
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:TEST", Price: 100, Volume: 5, Timestamp: base}))
	ts.failures = upsertMaxAttempts
	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:TEST", Price: 90, Volume: 1, Timestamp: base.Add(time.Minute)}))

	must(t, agg.Flush(ctx))
	if _, ok := ts.get("SIM:TEST", "1m", base); !ok {
		t.Errorf("Flush did not write the candle parked after a failed close")
	}
	if _, ok := ts.get("SIM:TEST", "1m", base.Add(time.Minute)); !ok {
		t.Errorf("Flush did not write the open bucket")
	}
}

func TestAggregator_DailyBarOnlyForSimulatedSymbols(t *testing.T) {
	ts := newFakeCandleStore()
	agg := newTestAggregator(&fakeRedis{}, ts)
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:TEST", Price: 100, Volume: 1, Timestamp: base, Simulated: true}))
	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "AAPL", Price: 200, Volume: 1, Timestamp: base, Simulated: false}))
	must(t, agg.Flush(ctx))

	day := base.Truncate(24 * time.Hour)
	if _, ok := ts.get("SIM:TEST", "1d", day); !ok {
		t.Errorf("simulated symbol should keep its aggregator 1d bar")
	}
	if _, ok := ts.get("AAPL", "1d", day); ok {
		t.Errorf("real symbol must not get an aggregator 1d bar: the daily sweep owns it")
	}
	for _, iv := range []string{"1m", "5m", "15m", "1h"} {
		if _, ok := ts.get("AAPL", iv, base.Truncate(map[string]time.Duration{"1m": time.Minute, "5m": 5 * time.Minute, "15m": 15 * time.Minute, "1h": time.Hour}[iv])); !ok {
			t.Errorf("real symbol should still get its %s bar", iv)
		}
	}
}

func TestAggregator_PublishesFinestEveryTick(t *testing.T) {
	ts := newFakeCandleStore()
	redis := &fakeRedis{}
	agg := newTestAggregator(redis, ts)
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := range 3 {
		must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:TEST", Price: 100 + float64(i), Volume: 1, Timestamp: base.Add(time.Duration(i) * time.Second)}))
	}
	redis.mu.Lock()
	defer redis.mu.Unlock()
	if len(redis.published) != 3 {
		t.Errorf("published %d candle events, want one per tick (3) with no DB write in between", len(redis.published))
	}
}

// vol returns a pointer to v — source.Candle.Volume is nil-able.
func vol(v float64) *float64 { return &v }

// sameCandle compares by value: Candle.Volume is a pointer, so == would
// compare addresses.
func sameCandle(a, b source.Candle) bool {
	av, bv := a.Volume, b.Volume
	a.Volume, b.Volume = nil, nil
	if a != b {
		return false
	}
	if av == nil || bv == nil {
		return av == bv
	}
	return *av == *bv
}

func fmtVol(p *float64) string {
	if p == nil {
		return "nil"
	}
	return strconv.FormatFloat(*p, 'g', -1, 64)
}
