package indicators

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
// candles on a miss, cache **per indicator** — one entry per
// (symbol, interval, name), the exact shape §6.3 calls
// cache:ind:{symbol}:{interval}:{set} with `set` narrowed to a single name.
// Caching per indicator rather than per requested combination means N
// indicators cost N cache entries, not up to 2ⁿ−1 — "ema,rsi" and "rsi"
// share ema's and rsi's entries instead of needlessly recomputing either.
// Each entry stores the full series — one cache entry serves both the
// latest-value response (Get) and the series response (GetSeries) for that
// indicator.
type Cache struct {
	redis *store.Redis
	ttl   time.Duration
}

func NewCache(redis *store.Redis) *Cache {
	return &Cache{redis: redis, ttl: DefaultTTL}
}

// GetSeries returns each requested indicator's full series, aligned to
// candles, computed from candles on a per-indicator cache miss. candles
// must already cover enough history for every requested indicator's period
// — the caller (the API handler) owns fetching them from Timescale. cached
// reports whether *every* requested indicator was served from Redis —
// false means at least one had to be computed fresh.
func (c *Cache) GetSeries(ctx context.Context, symbol, interval string, names []string, candles []source.Candle) (series map[string][]SeriesPoint, cached bool, err error) {
	series = make(map[string][]SeriesPoint, len(names))
	cached = true

	for _, name := range names {
		points, hit, err := c.getOne(ctx, symbol, interval, name, candles)
		if err != nil {
			return nil, false, err
		}
		if points == nil {
			continue // unknown indicator name, or not enough candle history yet
		}
		series[name] = points
		cached = cached && hit
	}

	return series, cached, nil
}

// getOne resolves a single indicator's series from its own cache entry,
// computing and caching it on a miss.
func (c *Cache) getOne(ctx context.Context, symbol, interval, name string, candles []source.Candle) (points []SeriesPoint, hit bool, err error) {
	entry, ok := Registry[name]
	if !ok {
		return nil, false, nil // unknown indicator name — silently skipped, not a hard error
	}

	key := indicatorCacheKey(symbol, interval, name)
	if raw, err := c.redis.CacheGet(ctx, key); err == nil {
		var v []SeriesPoint
		if jsonErr := json.Unmarshal([]byte(raw), &v); jsonErr == nil {
			return v, true, nil
		}
		// Fall through to recompute on a corrupt/unexpected cache entry
		// rather than failing the request outright.
	} else if !errors.Is(err, store.ErrCacheMiss) {
		return nil, false, fmt.Errorf("indicators: cache get: %w", err)
	}

	values, ok := entry.Series(candles)
	if !ok {
		return nil, false, nil // not enough candle history yet for this period
	}
	// values[i] aligns to candles[offset+i] — every Series implementation
	// returns one value per candle from its seed point onward, so the
	// offset is just the length difference.
	offset := len(candles) - len(values)
	points = make([]SeriesPoint, len(values))
	for i, v := range values {
		points[i] = SeriesPoint{Time: candles[offset+i].Time, Value: v}
	}

	if payload, err := json.Marshal(points); err == nil {
		if err := c.redis.CacheSet(ctx, key, string(payload), c.ttl); err != nil {
			// Non-fatal — the caller still gets a correct freshly-computed
			// result even if Redis is temporarily unavailable.
			return points, false, nil
		}
	}

	return points, false, nil
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

// indicatorCacheKey names one indicator's own cache entry — every requested
// `set` combination that includes `name` shares this same key, which is the
// whole point of caching per indicator rather than per set.
func indicatorCacheKey(symbol, interval, name string) string {
	return store.IndicatorCachePrefix(symbol, interval) + name
}
