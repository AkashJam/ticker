package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/AkashJam/ticker/internal/store"
)

const (
	leaderLockKey = "lock:ingest-leader"
	leaderTTL     = 15 * time.Second
	leaderRetry   = 3 * time.Second
)

// Leader elects a single ingesting instance via Redis (SET NX PX, §6.3) so
// that with more than one ticker replica running, only one actually
// produces to the Stream — every replica's aggregator still consumes from
// it. At v1's actual scale (one EC2 box, one ticker container) there is
// only ever one candidate, so this is non-load-bearing today; it's built
// anyway because it's cheap and is exactly the shape ADR-003's revisit
// trigger (splitting ingestion across replicas) already needs.
type Leader struct {
	redis *store.Redis
	token string
	held  atomic.Bool
	log   *slog.Logger
}

func NewLeader(redis *store.Redis, log *slog.Logger) *Leader {
	return &Leader{
		redis: redis,
		token: fmt.Sprintf("%s-%d-%d", hostname(), os.Getpid(), time.Now().UnixNano()),
		log:   log,
	}
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

// IsLeader reports whether this instance currently holds the lock.
func (l *Leader) IsLeader() bool {
	return l.held.Load()
}

// Run drives acquisition/renewal until ctx is canceled: retries acquiring
// the lock while standby, renews it on a cadence well inside its TTL while
// held. Intended to run in its own goroutine for the service's lifetime.
func (l *Leader) Run(ctx context.Context) {
	ticker := time.NewTicker(leaderRetry)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.tick(ctx)
		}
	}
}

func (l *Leader) tick(ctx context.Context) {
	if !l.held.Load() {
		ok, err := l.redis.AcquireLock(ctx, leaderLockKey, l.token, leaderTTL)
		if err != nil {
			l.log.Error("leader: acquire failed", "error", err)
			return
		}
		if ok {
			l.held.Store(true)
			l.log.Info("leader: acquired ingest lock", "token", l.token)
		}
		return
	}

	// Held: renew well before the TTL lapses (leaderRetry ticks every 3s,
	// TTL is 15s — renewing on every tick while held is comfortably inside
	// budget without needing a second ticker).
	ok, err := l.redis.RenewLock(ctx, leaderLockKey, l.token, leaderTTL)
	if err != nil {
		l.log.Error("leader: renew failed", "error", err)
		return
	}
	if !ok {
		// Lost the lock (e.g. a renewal was missed long enough for TTL to
		// lapse and another instance acquired it) — stop ingesting
		// immediately rather than believing we're still leader.
		l.held.Store(false)
		l.log.Warn("leader: lost ingest lock")
	}
}

// Release gives up leadership immediately, called from the shutdown path
// (portfolio.md §15) rather than left to TTL expiry, so a clean deploy
// hands leadership to the next instance with no ~15s gap.
func (l *Leader) Release(ctx context.Context) error {
	if !l.held.Load() {
		return nil
	}
	l.held.Store(false)
	if err := l.redis.ReleaseLock(ctx, leaderLockKey, l.token); err != nil {
		return fmt.Errorf("ingest: release leader lock: %w", err)
	}
	return nil
}
