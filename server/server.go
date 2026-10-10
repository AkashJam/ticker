// Package server wires the whole pipeline together: opens the stores,
// connects source → ingest → aggregate → API/SSE, and owns startup and
// graceful shutdown (portfolio.md §5).
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/AkashJam/ticker/flags"
	"github.com/AkashJam/ticker/internal/aggregate"
	"github.com/AkashJam/ticker/internal/api"
	"github.com/AkashJam/ticker/internal/indicators"
	"github.com/AkashJam/ticker/internal/ingest"
	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/sse"
	"github.com/AkashJam/ticker/internal/store"
	"github.com/AkashJam/ticker/internal/sweep"
)

// sseMaxConns is the connection cap resolved during planning (§13):
// reject-new (503) once this many SSE clients are connected.
const sseMaxConns = 200

// shutdownTimeout bounds how long graceful shutdown waits for in-flight
// work before giving up — long enough for the pipeline goroutines to
// notice cancellation and for a final candle flush, short enough that a
// stuck shutdown doesn't hang a deploy indefinitely.
const shutdownTimeout = 10 * time.Second

type Server struct {
	cfg flags.Config
	log *slog.Logger

	pool  interface{ Close() }
	redis *store.Redis
	ts    *store.Timescale

	src        source.MarketSource
	leader     *ingest.Leader
	producer   *ingest.Producer
	deadman    *ingest.DeadMan
	sweeper    *sweep.Scheduler // nil when no FINNHUB_API_KEY is set
	aggregator *aggregate.Aggregator
	httpServer *http.Server

	wg         sync.WaitGroup
	cancelPipe context.CancelFunc
	startedAt  time.Time
}

// New opens every dependency and wires the pipeline, but doesn't start
// anything yet — see Run.
func New(ctx context.Context, cfg flags.Config, log *slog.Logger) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	pool, err := store.NewPool(ctx, cfg.TimescaleDSN)
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}

	redis := store.NewRedis(cfg.RedisAddr)
	if err := redis.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("server: redis: %w", err)
	}

	ts := store.NewTimescale(pool)
	meta := store.NewMeta(pool)

	var src source.MarketSource
	switch cfg.Source {
	case "sim":
		src = source.NewSim()
	case "finnhub":
		// §16.1 — deferred. Fails loudly at startup rather than silently
		// falling back to sim, so selecting it is never a silent no-op.
		pool.Close()
		return nil, errors.New(`server: --source finnhub is not implemented yet (portfolio.md §16.1) — use "sim"`)
	default:
		pool.Close()
		return nil, fmt.Errorf("server: unknown source %q", cfg.Source)
	}

	leader := ingest.NewLeader(redis, log)
	deadman := ingest.NewDeadMan(cfg.HealthchecksURL)
	producer := ingest.NewProducer(redis, leader, log)

	aggregator := aggregate.New(redis, ts, aggWindowLabel(cfg.AggWindow), cfg.AggWindow, cfg.Consumer, log)

	cache := indicators.NewCache(redis)
	hub := sse.NewHub(redis, sseMaxConns, log)

	handlers := api.NewHandlers(meta, ts, redis, cache, nil, log)
	router := api.NewRouter(handlers, hub)

	// The daily-bar sweep (portfolio.md §15 Phase 8) is independent of
	// --source: it reads Finnhub's /quote for the 28 real symbols whatever
	// the live feed is. Without a key it is off, loudly, so dev and CI (which
	// have none) keep working.
	var sweeper *sweep.Scheduler
	if cfg.FinnhubAPIKey == "" {
		log.Warn("server: daily-bar sweep disabled — FINNHUB_API_KEY is not set")
	} else {
		loc, err := time.LoadLocation("America/New_York")
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("server: sweep: load America/New_York: %w", err)
		}
		runner := sweep.NewRunner(sweep.NewFinnhubClient(cfg.FinnhubAPIKey), ts, source.SweepSymbolCodes(), loc, log)
		sweeper = sweep.NewScheduler(runner, leader, sweep.NewPinger(cfg.HealthchecksSweepURL, log), log)
	}

	return &Server{
		cfg: cfg, log: log,
		pool: pool, redis: redis, ts: ts,
		src: src, leader: leader, producer: producer, deadman: deadman, sweeper: sweeper, aggregator: aggregator,
		httpServer: &http.Server{Addr: cfg.Addr, Handler: router},
	}, nil
}

// maxTickAge is how long the leader may go without ingesting a tick before
// the dead-man switch stops pinging. Generous on purpose: a quiet upstream
// or reconnect shouldn't page, only a genuinely stuck ingest loop.
const maxTickAge = 30 * time.Minute

// healthy is the dead-man switch's liveness check (§13): Redis must be
// reachable on every replica, and the leader must also have ingested
// recently. A leader that hasn't ticked since startup is measured from
// startedAt, so a fresh deploy doesn't false-alert.
func (s *Server) healthy(ctx context.Context) bool {
	if err := s.redis.Ping(ctx); err != nil {
		return false
	}
	if !s.leader.IsLeader() {
		return true
	}
	last := s.producer.LastTick()
	if last.IsZero() {
		last = s.startedAt
	}
	return time.Since(last) < maxTickAge
}

// Run starts the pipeline and the HTTP server, blocking until ctx is
// canceled (e.g. by a SIGTERM handler in cmd/main.go) or the server fails.
// Performs the §15 graceful shutdown itself before returning.
func (s *Server) Run(ctx context.Context) error {
	s.startedAt = time.Now()
	pipeCtx, cancel := context.WithCancel(ctx)
	s.cancelPipe = cancel

	symbols := symbolCodes()
	ticks, err := s.src.Subscribe(pipeCtx, symbols)
	if err != nil {
		return fmt.Errorf("server: subscribe: %w", err)
	}

	s.wg.Add(4)
	if s.sweeper != nil {
		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.sweeper.Run(pipeCtx) }()
	}
	go func() { defer s.wg.Done(); s.leader.Run(pipeCtx) }()
	go func() { defer s.wg.Done(); s.producer.Run(pipeCtx, ticks) }()
	go func() { defer s.wg.Done(); s.deadman.Run(pipeCtx, s.healthy) }()
	go func() {
		defer s.wg.Done()
		if err := s.aggregator.Run(pipeCtx); err != nil {
			s.log.Error("server: aggregator stopped", "error", err)
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		s.log.Info("server: listening", "addr", s.cfg.Addr, "source", s.cfg.Source, "env", s.cfg.Env)
		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("server: listen: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		return s.shutdown()
	case err := <-errCh:
		_ = s.shutdown()
		return err
	}
}

// shutdown implements §15's requirement precisely: the HTTP server stops
// accepting new work, the pipeline goroutines are canceled and awaited,
// then — and only then — the aggregator flushes in-flight candles and the
// ingestion loop releases its leader lock, so neither races against a
// goroutine that might still be mutating the same state.
//
// a ctx from the caller — by the time shutdown runs, the caller's ctx is
// either already canceled (the ctx.Done() path) or irrelevant (the
// listener-error path), and Redis/Timescale calls here need a live context
// to actually complete.
//
//nolint:contextcheck // deliberately uses a fresh context.Background(), not
func (s *Server) shutdown() error {
	s.log.Info("server: shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.log.Error("server: http shutdown", "error", err)
	}

	if s.cancelPipe != nil {
		s.cancelPipe()
	}
	if !s.waitWithTimeout(ctx) {
		s.log.Warn("server: pipeline goroutines did not stop before the shutdown timeout")
	}

	if err := s.aggregator.Flush(ctx); err != nil {
		s.log.Error("server: flush candles", "error", err)
	}
	if err := s.leader.Release(ctx); err != nil {
		s.log.Error("server: release leader lock", "error", err)
	}

	s.pool.Close()
	if err := s.redis.Close(); err != nil {
		s.log.Error("server: close redis", "error", err)
	}

	return nil
}

func (s *Server) waitWithTimeout(ctx context.Context) bool {
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

func symbolCodes() []string {
	codes := make([]string, len(source.Symbols))
	for i, s := range source.Symbols {
		codes[i] = s.Symbol
	}
	return codes
}

// aggWindowLabel renders --agg-window as the interval label the DB/API use
// ("10s", "1m", ...) — Go's Duration.String() would print "1m0s" instead.
func aggWindowLabel(d time.Duration) string {
	switch {
	case d >= time.Hour && d%time.Hour == 0:
		return fmt.Sprintf("%dh", int(d/time.Hour))
	case d >= time.Minute && d%time.Minute == 0:
		return fmt.Sprintf("%dm", int(d/time.Minute))
	default:
		return fmt.Sprintf("%ds", int(d/time.Second))
	}
}
