// Package reference is the Atlas reference store (portfolio.md ADR-008): JSON
// blobs served through the Redis cache path, with an embedded snapshot as the
// floor for cold start, a source outage, and offline dev and CI. It never
// touches the Redis Stream, the leader lease or the aggregator.
package reference

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"time"

	"github.com/AkashJam/ticker/internal/store"
)

// DefaultTTL is a staleness ceiling, not a freshness mechanism: the data
// refreshes daily to yearly, and the fetchers overwrite it well before this.
const DefaultTTL = 30 * 24 * time.Hour

const keyPrefix = "ref:"

// Cache is the slice of *store.Redis the store needs. Taking an interface lets
// the Redis-down gate be tested with a failing fake.
type Cache interface {
	CacheGet(ctx context.Context, key string) (string, error)
	CacheSet(ctx context.Context, key, value string, ttl time.Duration) error
}

// Source says which layer answered a Get.
type Source string

const (
	SourceLive     Source = "live"
	SourceEmbedded Source = "embedded"
)

// CacheKey is the Redis key for a dataset. The prefix cannot collide with
// quote:latest:* or cache:ind:*.
func CacheKey(name string) string { return keyPrefix + name }

// Store reads a dataset from Redis and falls back to the embedded snapshot.
type Store struct {
	cache Cache
	fsys  fs.FS
	ttl   time.Duration
	log   *slog.Logger
}

// New builds a Store. cache may be nil (offline dev), in which case every Get
// is served from fsys and Put fails. A nil log uses slog.Default.
func New(cache Cache, fsys fs.FS, log *slog.Logger) *Store {
	if log == nil {
		log = slog.Default()
	}
	return &Store{cache: cache, fsys: fsys, ttl: DefaultTTL, log: log}
}

// Get decodes dataset name into v. Redis is tried first; a miss, an error or a
// corrupt payload degrades to the embedded snapshot. It errors only when the
// embedded snapshot is itself missing or invalid.
func (s *Store) Get(ctx context.Context, name string, v any) (Source, error) {
	if s.cache != nil {
		raw, err := s.cache.CacheGet(ctx, CacheKey(name))
		switch {
		case err == nil:
			jerr := json.Unmarshal([]byte(raw), v)
			if jerr == nil {
				return SourceLive, nil
			}
			s.log.Warn("reference: corrupt cached payload, using embedded", "dataset", name, "err", jerr)
		case errors.Is(err, store.ErrCacheMiss):
			// Cold start: expected, not worth a log line.
		default:
			s.log.Warn("reference: cache unavailable, using embedded", "dataset", name, "err", err)
		}
	}

	b, err := fs.ReadFile(s.fsys, name+".json")
	if err != nil {
		return "", fmt.Errorf("reference: embedded %s: %w", name, err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return "", fmt.Errorf("reference: embedded %s: %w", name, err)
	}
	return SourceEmbedded, nil
}

// Put stores dataset name in Redis for the fetchers to call after a refresh.
func (s *Store) Put(ctx context.Context, name string, v any) error {
	if s.cache == nil {
		return errors.New("reference: no cache configured")
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("reference: marshal %s: %w", name, err)
	}
	return s.cache.CacheSet(ctx, CacheKey(name), string(b), s.ttl)
}
