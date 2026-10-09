package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/store"
)

// ticksProduced is the one custom business metric alongside the process/Go
// runtime metrics promhttp.Handler() exposes automatically on /metrics.
var ticksProduced = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ticker_ticks_produced_total",
	Help: "Ticks successfully written to the Redis Stream, by symbol.",
}, []string{"symbol"})

const (
	ticksStream = "ticks:raw"
	quoteTTL    = 24 * time.Hour // snapshot cache outlives any single tick; refreshed on every tick anyway
)

// RedisPort is everything Producer needs from Redis — narrowed to an
// interface so the leader-gating/validation logic in handle can be unit
// tested without a real Redis connection. *store.Redis satisfies it.
type RedisPort interface {
	XAdd(ctx context.Context, stream string, values map[string]any) error
	Publish(ctx context.Context, channel string, payload []byte) error
	CacheSet(ctx context.Context, key, value string, ttl time.Duration) error
}

// LeaderChecker is the one thing Producer needs from Leader.
type LeaderChecker interface {
	IsLeader() bool
}

// Producer is the leader-gated stage that pushes validated ticks onto the
// Redis Stream and, in parallel, publishes a live quote for the SSE hub —
// portfolio.md §6.1's "XADD ticks:raw" and "PUBLISH quotes:{sym}" both
// happen here, from the single point that already knows this instance is
// (or isn't) the ingest leader.
type Producer struct {
	redis    RedisPort
	leader   LeaderChecker
	log      *slog.Logger
	lastTick atomic.Int64 // unix nanos of the last tick written to the Stream; 0 if none yet
}

func NewProducer(redis RedisPort, leader LeaderChecker, log *slog.Logger) *Producer {
	return &Producer{redis: redis, leader: leader, log: log}
}

// LastTick is when this producer last wrote a tick to the Stream, or the
// zero time if it hasn't yet. Feeds the dead-man health check (§13).
func (p *Producer) LastTick() time.Time {
	n := p.lastTick.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// Run consumes ticks until the channel closes (on source-side ctx
// cancellation) or ctx is canceled directly.
func (p *Producer) Run(ctx context.Context, ticks <-chan source.NormalizedTick) {
	for {
		select {
		case <-ctx.Done():
			return
		case tick, ok := <-ticks:
			if !ok {
				return
			}
			p.handle(ctx, tick)
		}
	}
}

func (p *Producer) handle(ctx context.Context, tick source.NormalizedTick) {
	if !p.leader.IsLeader() {
		return // standby replica — no ingest (§6.1)
	}

	tick, err := Normalize(tick)
	if err != nil {
		p.log.Warn("ingest: dropping invalid tick", "error", err)
		return
	}

	payload, err := json.Marshal(tick)
	if err != nil {
		p.log.Error("ingest: marshal tick", "error", err)
		return
	}

	if err := p.redis.XAdd(ctx, ticksStream, map[string]any{"data": payload}); err != nil {
		p.log.Error("ingest: xadd failed", "symbol", tick.Symbol, "error", err)
		return
	}
	ticksProduced.WithLabelValues(tick.Symbol).Inc()
	p.lastTick.Store(time.Now().UnixNano())

	envelope, err := json.Marshal(source.PubSubMessage{Type: "quote", Payload: payload})
	if err != nil {
		p.log.Error("ingest: marshal quote envelope", "error", err)
	} else if err := p.redis.Publish(ctx, store.QuoteChannel(tick.Symbol), envelope); err != nil {
		p.log.Error("ingest: publish failed", "symbol", tick.Symbol, "error", err)
		// Not fatal — the tick is already durably on the Stream; only the
		// live SSE push for this one update is lost, not the data itself.
	}

	if err := p.redis.CacheSet(ctx, store.QuoteCacheKey(tick.Symbol), string(payload), quoteTTL); err != nil {
		p.log.Error("ingest: quote cache set failed", "symbol", tick.Symbol, "error", err)
	}
}
