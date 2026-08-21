package aggregate

import (
	"context"
	"io"
	"log/slog"
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

type fakeCandleStore struct {
	mu     sync.Mutex
	latest map[candleKey]source.Candle
	calls  int
}

func newFakeCandleStore() *fakeCandleStore {
	return &fakeCandleStore{latest: make(map[candleKey]source.Candle)}
}

func (f *fakeCandleStore) UpsertCandle(_ context.Context, c source.Candle) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.latest[candleKey{c.Symbol, c.Interval, c.Time}] = c
	return nil
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
	return New(redis, ts, "1m", time.Minute, log)
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

	got, ok := ts.get("SIM:TEST", "1m", base)
	if !ok {
		t.Fatalf("expected an upserted 1m candle")
	}
	want := source.Candle{Time: base, Symbol: "SIM:TEST", Interval: "1m", Open: 100, High: 105, Low: 98, Close: 98, Volume: 45}
	if got != want {
		t.Errorf("candle = %+v, want %+v", got, want)
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
		t.Fatalf("expected the closed bucket's candle to remain in the store")
	}
	wantClosed := source.Candle{Time: base, Symbol: "SIM:TEST", Interval: "1m", Open: 100, High: 110, Low: 100, Close: 110, Volume: 10}
	if closed != wantClosed {
		t.Errorf("closed candle = %+v, want %+v", closed, wantClosed)
	}

	opened, ok := ts.get("SIM:TEST", "1m", next.Truncate(time.Minute))
	if !ok {
		t.Fatalf("expected the new bucket's candle to exist")
	}
	wantOpened := source.Candle{Time: next.Truncate(time.Minute), Symbol: "SIM:TEST", Interval: "1m", Open: 90, High: 90, Low: 90, Close: 90, Volume: 7}
	if opened != wantOpened {
		t.Errorf("new candle = %+v, want a fresh bar seeded from the closing tick: %+v", opened, wantOpened)
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
	// ADR-004 / Risk Register #4: candle upsert is idempotent by
	// (symbol, interval, time) — writing the same candle state repeatedly
	// (as processTick does on every tick within a still-open bucket, and as
	// a redelivered Stream entry could trigger) must never produce more
	// than one stored row for that key.
	ts := newFakeCandleStore()
	ctx := context.Background()
	c := source.Candle{Symbol: "SIM:TEST", Interval: "1m", Time: time.Unix(0, 0).UTC(), Open: 100, High: 105, Low: 98, Close: 102, Volume: 40}

	must(t, ts.UpsertCandle(ctx, c))
	must(t, ts.UpsertCandle(ctx, c)) // repeat write of the identical state

	if ts.calls != 2 {
		t.Fatalf("expected 2 UpsertCandle calls, got %d", ts.calls)
	}
	if len(ts.latest) != 1 {
		t.Errorf("expected exactly one stored row for the (symbol,interval,time) key, got %d", len(ts.latest))
	}
	got, ok := ts.get(c.Symbol, c.Interval, c.Time)
	if !ok || got != c {
		t.Errorf("stored candle = %+v, want %+v", got, c)
	}
}

func TestAggregator_Flush(t *testing.T) {
	ts := newFakeCandleStore()
	agg := newTestAggregator(&fakeRedis{}, ts)
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:A", Price: 50, Volume: 1, Timestamp: base}))
	must(t, agg.processTick(ctx, source.NormalizedTick{Symbol: "SIM:B", Price: 60, Volume: 1, Timestamp: base}))

	callsBefore := ts.calls
	must(t, agg.Flush(ctx))
	if ts.calls <= callsBefore {
		t.Errorf("expected Flush to issue additional upserts: calls before=%d after=%d", callsBefore, ts.calls)
	}

	for _, sym := range []string{"SIM:A", "SIM:B"} {
		if _, ok := ts.get(sym, "1m", base); !ok {
			t.Errorf("Flush: expected a persisted candle for %s/1m", sym)
		}
	}
}
