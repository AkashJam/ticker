package indicators

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/store"
)

// DefaultTTL bounds how stale a cached indicator value can be between
// candle closes — closes also proactively invalidate (internal/aggregate),
// so this is a ceiling, not the primary invalidation path.
const DefaultTTL = 5 * time.Minute

// Value is one indicator's latest result — the shape §8's default response
// nests under indicators.{name}.
type Value struct {
	Period int     `json:"period"`
	Value  float64 `json:"value"`
}

// SeriesPoint pairs one indicator value with the candle it's aligned to —
// the §8 `series=true` response shape.
type SeriesPoint struct {
	Time  time.Time `json:"time"`
	Value float64   `json:"value"`
}

// Cache implements §9.4's compute-on-read path: check Redis, compute from
// candles on a miss, cache the combined result under one key per (symbol,
// interval, set) — the exact shape §6.3 calls cache:ind:{symbol}:{interval}:{set}.
// It always stores the full series — one cache entry serves both the
// latest-value response (Get) and the series response (GetSeries).
type Cache struct {
	redis *store.Redis
	ttl   time.Duration
}

func NewCache(redis *store.Redis) *Cache {
	return &Cache{redis: redis, ttl: DefaultTTL}
}

// GetSeries returns each requested indicator's full series, aligned to
// candles, computed from candles on a cache miss. candles must already
// cover enough history for every requested indicator's period — the
// caller (the API handler) owns fetching them from Timescale. cached
// reports whether the result came from Redis rather than being freshly
// computed.
func (c *Cache) GetSeries(ctx context.Context, symbol, interval string, names []string, candles []source.Candle) (series map[string][]SeriesPoint, cached bool, err error) {
	key := cacheKey(symbol, interval, names)

	if raw, err := c.redis.CacheGet(ctx, key); err == nil {
		var v map[string][]SeriesPoint
		if jsonErr := json.Unmarshal([]byte(raw), &v); jsonErr == nil {
			return v, true, nil
		}
		// Fall through to recompute on a corrupt/unexpected cache entry
		// rather than failing the request outright.
	} else if !errors.Is(err, store.ErrCacheMiss) {
		return nil, false, fmt.Errorf("indicators: cache get: %w", err)
	}

	series = make(map[string][]SeriesPoint, len(names))
	for _, name := range names {
		entry, ok := Registry[name]
		if !ok {
			continue // unknown indicator name — silently skipped, not a hard error
		}
		values, ok := entry.Series(candles)
		if !ok {
			continue // not enough candle history yet for this period
		}
		// values[i] aligns to candles[offset+i] — every Series
		// implementation returns one value per candle from its seed point
		// onward, so the offset is just the length difference.
		offset := len(candles) - len(values)
		points := make([]SeriesPoint, len(values))
		for i, v := range values {
			points[i] = SeriesPoint{Time: candles[offset+i].Time, Value: v}
		}
		series[name] = points
	}

	if payload, err := json.Marshal(series); err == nil {
		if err := c.redis.CacheSet(ctx, key, string(payload), c.ttl); err != nil {
			// Non-fatal — the caller still gets a correct freshly-computed
			// result even if Redis is temporarily unavailable.
			return series, false, nil
		}
	}

	return series, false, nil
}

// Get returns just the latest value per requested indicator — the
// existing §8 default response shape — derived from GetSeries.
func (c *Cache) Get(ctx context.Context, symbol, interval string, names []string, candles []source.Candle) (values map[string]Value, cached bool, err error) {
	series, cached, err := c.GetSeries(ctx, symbol, interval, names, candles)
	if err != nil {
		return nil, false, err
	}

	values = make(map[string]Value, len(series))
	for name, points := range series {
		if len(points) == 0 {
			continue
		}
		values[name] = Value{Period: Registry[name].Period, Value: points[len(points)-1].Value}
	}
	return values, cached, nil
}

// cacheKey normalizes the requested indicator names (sorted, deduped by
// sort) so "rsi,ema" and "ema,rsi" share one cache entry instead of two.
func cacheKey(symbol, interval string, names []string) string {
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)
	return store.IndicatorCachePrefix(symbol, interval) + strings.Join(sorted, ",")
}
