package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrCacheMiss is returned by CacheGet when the key doesn't exist — callers
// (internal/indicators/cache.go) branch on this to trigger the
// compute-on-read path (§9.4).
var ErrCacheMiss = errors.New("store: cache miss")

// releaseIfOwner / renewIfOwner are Lua scripts so the compare-and-delete /
// compare-and-extend are atomic — a plain GET-then-DEL/EXPIRE from Go would
// race against another process's lock acquisition between the two calls.
const (
	releaseIfOwnerScript = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		end
		return 0`
	renewIfOwnerScript = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
		end
		return 0`
)

// QuoteChannel names the pub/sub channel a symbol's quote/candle updates
// publish to (§6.3: quotes:{symbol}) — one channel per symbol carries both
// message types, distinguished by source.PubSubMessage.Type.
func QuoteChannel(symbol string) string {
	return "quotes:" + symbol
}

// QuoteCacheKey names the key holding a symbol's last-seen tick (§6.3:
// quote:latest:{symbol}) — written by internal/ingest on every tick, read
// by the snapshot and movers API handlers.
func QuoteCacheKey(symbol string) string {
	return "quote:latest:" + symbol
}

// IndicatorCachePrefix names the cache-key prefix for one (symbol,
// interval)'s indicator cache entries (§6.3:
// cache:ind:{symbol}:{interval}:{set}) — every `set` combination for that
// pair shares this prefix, so CacheDelPrefix(ctx, IndicatorCachePrefix(...))
// invalidates all of them on candle close regardless of which sets clients
// have queried.
func IndicatorCachePrefix(symbol, interval string) string {
	return fmt.Sprintf("cache:ind:%s:%s:", symbol, interval)
}

// Redis wraps the primitives internal/ingest (lock, stream), internal/sse
// (pub/sub), and internal/indicators (cache) each build their domain logic
// on top of (portfolio.md §5: "redis.go — pub/sub, cache, lock").
type Redis struct {
	Client *redis.Client
}

func NewRedis(addr string) *Redis {
	return &Redis{Client: redis.NewClient(&redis.Options{Addr: addr})}
}

func (r *Redis) Ping(ctx context.Context) error {
	return r.Client.Ping(ctx).Err()
}

func (r *Redis) Close() error {
	return r.Client.Close()
}

// --- Leader lock (lock:ingest-leader) ---

// AcquireLock attempts SET key token NX PX ttl — true if this call won the
// lock.
func (r *Redis) AcquireLock(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	ok, err := r.Client.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("store: acquire lock: %w", err)
	}
	return ok, nil
}

// RenewLock extends the lock's TTL, but only if `token` still owns it —
// prevents a stalled goroutine from renewing a lock another process has
// since acquired after this one's TTL lapsed.
func (r *Redis) RenewLock(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	res, err := r.Client.Eval(ctx, renewIfOwnerScript, []string{key}, token, ttl.Milliseconds()).Result()
	if err != nil {
		return false, fmt.Errorf("store: renew lock: %w", err)
	}
	return res.(int64) == 1, nil
}

// ReleaseLock deletes the lock, but only if `token` still owns it — called
// explicitly from the shutdown path (portfolio.md §15) rather than left to
// TTL expiry, so a clean shutdown hands leadership over immediately.
func (r *Redis) ReleaseLock(ctx context.Context, key, token string) error {
	_, err := r.Client.Eval(ctx, releaseIfOwnerScript, []string{key}, token).Result()
	if err != nil {
		return fmt.Errorf("store: release lock: %w", err)
	}
	return nil
}

// --- Stream (ticks:raw) ---

func (r *Redis) XAdd(ctx context.Context, stream string, values map[string]any) error {
	err := r.Client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: values}).Err()
	if err != nil {
		return fmt.Errorf("store: xadd: %w", err)
	}
	return nil
}

// StreamMessage is a stripped-down view of a consumer-group entry — just
// enough for internal/aggregate, so it doesn't need to import go-redis
// directly.
type StreamMessage struct {
	ID   string
	Data string
}

// EnsureConsumerGroup creates the group starting from new entries only
// (`$`) if it doesn't already exist; a pre-existing group (BUSYGROUP) is
// not an error — every service restart hits this on the same group.
func (r *Redis) EnsureConsumerGroup(ctx context.Context, stream, group string) error {
	err := r.Client.XGroupCreateMkStream(ctx, stream, group, "$").Err()
	if err == nil || strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return nil
	}
	return fmt.Errorf("store: create consumer group: %w", err)
}

// ReadGroup blocks up to `block` for new entries, returning nil (not an
// error) on a timeout with nothing new — the normal steady-state case in a
// low-throughput stream.
func (r *Redis) ReadGroup(ctx context.Context, stream, group, consumer string, count int64, block time.Duration) ([]StreamMessage, error) {
	res, err := r.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: consumer,
		Streams: []string{stream, ">"},
		Count:   count, Block: block,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("store: read group: %w", err)
	}

	var out []StreamMessage
	for _, s := range res {
		for _, m := range s.Messages {
			data, _ := m.Values["data"].(string)
			out = append(out, StreamMessage{ID: m.ID, Data: data})
		}
	}
	return out, nil
}

// AutoClaim claims entries idle for at least minIdle from other consumers in
// the group (content/REVIEW.md #2/#4: makes redelivery real, rather than
// leaving crashed/stalled consumers' pending entries stranded forever).
// start is the cursor to resume scanning the group's PEL from ("0" scans
// from the beginning); next is passed back in as start on the following
// call to page through a large PEL rather than reclaiming the same prefix
// repeatedly.
func (r *Redis) AutoClaim(ctx context.Context, stream, group, consumer string, minIdle time.Duration, start string, count int64) (msgs []StreamMessage, next string, err error) {
	xmsgs, next, err := r.Client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: stream, Group: group, Consumer: consumer,
		MinIdle: minIdle, Start: start, Count: count,
	}).Result()
	if err != nil {
		return nil, "", fmt.Errorf("store: xautoclaim: %w", err)
	}

	out := make([]StreamMessage, 0, len(xmsgs))
	for _, m := range xmsgs {
		data, _ := m.Values["data"].(string)
		out = append(out, StreamMessage{ID: m.ID, Data: data})
	}
	return out, next, nil
}

// Ack acknowledges processed entries so they aren't redelivered.
func (r *Redis) Ack(ctx context.Context, stream, group string, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := r.Client.XAck(ctx, stream, group, ids...).Err(); err != nil {
		return fmt.Errorf("store: ack: %w", err)
	}
	return nil
}

// --- Pub/Sub (quotes:{symbol}) ---

func (r *Redis) Publish(ctx context.Context, channel string, payload []byte) error {
	if err := r.Client.Publish(ctx, channel, payload).Err(); err != nil {
		return fmt.Errorf("store: publish: %w", err)
	}
	return nil
}

func (r *Redis) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return r.Client.Subscribe(ctx, channels...)
}

// --- Cache (quote:latest:{symbol}, cache:ind:{symbol}:{interval}:{set}) ---

func (r *Redis) CacheSet(ctx context.Context, key, value string, ttl time.Duration) error {
	if err := r.Client.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("store: cache set: %w", err)
	}
	return nil
}

func (r *Redis) CacheGet(ctx context.Context, key string) (string, error) {
	v, err := r.Client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrCacheMiss
	}
	if err != nil {
		return "", fmt.Errorf("store: cache get: %w", err)
	}
	return v, nil
}

// CacheDel invalidates specific keys.
func (r *Redis) CacheDel(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := r.Client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("store: cache del: %w", err)
	}
	return nil
}

// CacheDelPrefix invalidates every key matching prefix+"*" — used on candle
// close to drop every cached indicator `set` combination for that (symbol,
// interval), since a client may have requested any subset (e.g. "ema",
// "rsi", "ema,rsi") and each gets its own cache key (§6.3:
// cache:ind:{symbol}:{interval}:{set}).
func (r *Redis) CacheDelPrefix(ctx context.Context, prefix string) error {
	iter := r.Client.Scan(ctx, 0, prefix+"*", 0).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("store: scan cache keys: %w", err)
	}
	return r.CacheDel(ctx, keys...)
}
