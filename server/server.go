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
	aggregator *aggregate.Aggregator
	httpServer *http.Server

	wg           sync.WaitGroup
	cancelPipe   context.CancelFunc
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
	producer := ingest.NewProducer(redis, leader, deadman, log)

	aggregator := aggregate.New(redis, ts, aggWindowLabel(cfg.AggWindow), cfg.AggWindow, log)

	cache := indicators.NewCache(redis)
	hub := sse.NewHub(redis, sseMaxConns, log)

	handlers := api.NewHandlers(meta, ts, redis, cache, nil, log)
	router := api.NewRouter(handlers, hub)

	return &Server{
		cfg: cfg, log: log,
		pool: pool, redis: redis, ts: ts,
		src: src, leader: leader, producer: producer, aggregator: aggregator,
		httpServer: &http.Server{Addr: cfg.Addr, Handler: router},
	}, nil
}

// Run starts the pipeline and the HTTP server, blocking until ctx is
// canceled (e.g. by a SIGTERM handler in cmd/main.go) or the server fails.
// Performs the §15 graceful shutdown itself before returning.
func (s *Server) Run(ctx context.Context) error {
	pipeCtx, cancel := context.WithCancel(ctx)
	s.cancelPipe = cancel

	symbols := symbolCodes()
	ticks, err := s.src.Subscribe(pipeCtx, symbols)
	if err != nil {
		return fmt.Errorf("server: subscribe: %w", err)
	}

	s.wg.Add(3)
	go func() { defer s.wg.Done(); s.leader.Run(pipeCtx) }()
	go func() { defer s.wg.Done(); s.producer.Run(pipeCtx, ticks) }()
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
//nolint:contextcheck // deliberately uses a fresh context.Background(), not
// a ctx from the caller — by the time shutdown runs, the caller's ctx is
// either already canceled (the ctx.Done() path) or irrelevant (the
// listener-error path), and Redis/Timescale calls here need a live context
// to actually complete.
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
