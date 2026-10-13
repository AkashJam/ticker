package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	finnhubWSURL = "wss://ws.finnhub.io"

	// barWidth is the coalescing window (portfolio.md §15 Phase 13): trades
	// are folded into one OHLCV bar per symbol per second before anything
	// reaches the stream, so the tick rate is bounded at symbols x 1/sec.
	barWidth = time.Second
	// coalesceGrace holds a bar open past its second so a trade the feed
	// delivers a little late still lands in its own bar.
	coalesceGrace = time.Second
	flushEvery    = 250 * time.Millisecond

	backoffMin = time.Second
	backoffMax = time.Minute
	// healthyAfter is how long a connection must last before the backoff
	// resets, so a feed that accepts and then drops us does not reset it.
	healthyAfter = time.Minute
	// readIdle bounds how long the socket may be silent. Finnhub sends a
	// ping frame about every ten seconds, so silence this long is a dead
	// connection, not a quiet market.
	readIdle = 90 * time.Second
	// readLimit is above the library's 32 KiB default: one frame carries every
	// trade since the last push, and a busy symbol's can be large.
	readLimit = 1 << 20
)

var (
	finnhubTrades = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ticker_finnhub_trades_total",
		Help: "Trades received from the Finnhub WebSocket.",
	})
	finnhubLateTrades = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ticker_finnhub_late_trades_total",
		Help: "Trades dropped because their one-second bar had already been emitted.",
	})
	finnhubConnects = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ticker_finnhub_connects_total",
		Help: "Successful Finnhub WebSocket connections, including reconnects.",
	})
)

// ErrUnauthorized is returned when Finnhub rejects the API key (HTTP 401 or
// 403 on the upgrade). It is not retried: a bad key does not heal.
var ErrUnauthorized = errors.New("finnhub: websocket rejected the API key")

// Finnhub is the live MarketSource: US trades over Finnhub's WebSocket,
// coalesced into 1-second OHLCV bars.
//
// Authentication is the one place this differs from the REST client in
// internal/sweep: Finnhub's WebSocket answers an X-Finnhub-Token header with
// HTTP 401 (measured 2026-10-13) and accepts only ?token=. The URL is
// therefore built inside dial and every error and log line passes through
// redact, so the key never reaches a log, a metric or a returned error.
type Finnhub struct {
	key    string
	wsURL  string
	assets map[string]AssetType
	log    *slog.Logger
	now    func() time.Time
}

// NewFinnhub returns a Finnhub source for the given key. universe supplies
// each streamed symbol's asset type.
func NewFinnhub(key string, universe []SymbolInfo, log *slog.Logger) *Finnhub {
	assets := make(map[string]AssetType, len(universe))
	for _, s := range universe {
		assets[s.Symbol] = s.Type
	}
	return &Finnhub{key: key, wsURL: finnhubWSURL, assets: assets, log: log, now: time.Now}
}

func (f *Finnhub) Name() string { return "finnhub" }

// Backfill returns nothing: /stock/candle is HTTP 403 on the free tier, so a
// gap after a reconnect is simply missing seconds (portfolio.md §15).
func (f *Finnhub) Backfill(_ context.Context, _ string, _, _ time.Time) ([]Candle, error) {
	return nil, nil
}

// Subscribe connects, subscribes to symbols and streams one tick per symbol
// per second until ctx is canceled, at which point the channel is closed. A
// rejected key on the first connection is returned as ErrUnauthorized, so a
// wrong key fails at startup; any other first-connection failure, and every
// later drop, is retried with exponential backoff.
func (f *Finnhub) Subscribe(ctx context.Context, symbols []string) (<-chan NormalizedTick, error) {
	conn, err := f.dial(ctx)
	if errors.Is(err, ErrUnauthorized) {
		return nil, err
	}
	if err != nil {
		f.log.Warn("finnhub: first connection failed, retrying", "error", f.redact(err))
	}

	out := make(chan NormalizedTick, 64)
	go f.run(ctx, symbols, conn, out)
	return out, nil
}

// run owns the connection loop and the flusher until ctx is canceled or the
// key is rejected, then closes out.
func (f *Finnhub) run(ctx context.Context, symbols []string, conn *websocket.Conn, out chan<- NormalizedTick) {
	runCtx, cancel := context.WithCancel(ctx)

	co := newCoalescer()
	var wg sync.WaitGroup
	wg.Go(func() { f.flushLoop(runCtx, co, out) })
	defer func() {
		cancel() // stop the flusher before waiting on it: a key rejection returns without ctx being done
		wg.Wait()
		close(out)
	}()

	backoff := backoffMin
	for runCtx.Err() == nil {
		if conn == nil {
			var err error
			if conn, err = f.dial(runCtx); err != nil {
				if errors.Is(err, ErrUnauthorized) {
					f.log.Error("finnhub: key rejected, stopping", "error", f.redact(err))
					return
				}
				if runCtx.Err() == nil {
					f.log.Warn("finnhub: connect failed", "error", f.redact(err), "retry_in", backoff)
				}
				sleepCtx(runCtx, jitter(backoff))
				backoff = min(backoff*2, backoffMax)
				continue
			}
		}

		started := f.now()
		err := f.session(runCtx, conn, symbols, co)
		_ = conn.CloseNow()
		conn = nil
		if runCtx.Err() != nil {
			return
		}
		if f.now().Sub(started) >= healthyAfter {
			backoff = backoffMin
		}
		f.log.Warn("finnhub: connection lost", "error", f.redact(err), "retry_in", backoff)
		sleepCtx(runCtx, jitter(backoff))
		backoff = min(backoff*2, backoffMax)
	}
}

// dial opens the WebSocket. The token travels in the query string because the
// header form is rejected (see Finnhub); nothing outside this function sees
// the URL.
func (f *Finnhub) dial(ctx context.Context) (*websocket.Conn, error) {
	//nolint:bodyclose // coder/websocket closes the handshake response body itself
	conn, resp, err := websocket.Dial(ctx, f.wsURL+"?token="+url.QueryEscape(f.key), nil)
	if err != nil {
		if resp != nil && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) {
			return nil, ErrUnauthorized
		}
		return nil, errors.New(f.redact(err))
	}
	conn.SetReadLimit(readLimit)
	finnhubConnects.Inc()
	return conn, nil
}

// session subscribes to every symbol and reads frames until the connection
// fails, the socket goes silent, or ctx is canceled.
func (f *Finnhub) session(ctx context.Context, conn *websocket.Conn, symbols []string, co *coalescer) error {
	for _, s := range symbols {
		msg, _ := json.Marshal(map[string]string{"type": "subscribe", "symbol": s})
		if err := conn.Write(ctx, websocket.MessageText, msg); err != nil {
			return fmt.Errorf("subscribe %s: %w", s, err)
		}
	}
	f.log.Info("finnhub: connected", "symbols", len(symbols))

	for {
		rctx, cancel := context.WithTimeout(ctx, readIdle)
		_, data, err := conn.Read(rctx)
		cancel()
		if err != nil {
			return err
		}
		f.handleFrame(data, co)
	}
}

type frame struct {
	Type string  `json:"type"`
	Msg  string  `json:"msg"`
	Data []trade `json:"data"`
}

// trade is one execution: p price, s symbol, t Unix milliseconds, v shares.
type trade struct {
	P float64 `json:"p"`
	S string  `json:"s"`
	T int64   `json:"t"`
	V float64 `json:"v"`
}

func (f *Finnhub) handleFrame(data []byte, co *coalescer) {
	var fr frame
	if err := json.Unmarshal(data, &fr); err != nil {
		f.log.Debug("finnhub: unreadable frame", "error", err)
		return
	}
	switch fr.Type {
	case "trade":
		for _, t := range fr.Data {
			if t.S == "" || t.P <= 0 || t.V < 0 {
				continue
			}
			finnhubTrades.Inc()
			if !co.add(t.S, t.P, t.V, time.UnixMilli(t.T)) {
				finnhubLateTrades.Inc()
			}
		}
	case "ping":
	case "error":
		f.log.Warn("finnhub: server error", "msg", fr.Msg)
	default:
		f.log.Debug("finnhub: unhandled frame", "type", fr.Type)
	}
}

// flushLoop emits every bar whose second, plus the grace, has passed.
func (f *Finnhub) flushLoop(ctx context.Context, co *coalescer, out chan<- NormalizedTick) {
	t := time.NewTicker(flushEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, b := range co.flush(f.now()) {
				select {
				case out <- f.tick(b):
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (f *Finnhub) tick(b bar) NormalizedTick {
	asset, ok := f.assets[b.symbol]
	if !ok {
		asset = AssetTypeStock
	}
	return NormalizedTick{
		Symbol: b.symbol, Price: b.last, Open: b.open, High: b.high, Low: b.low,
		Volume: b.volume, Timestamp: b.second, AssetType: asset,
	}
}

// redact strips the API key, raw and URL-escaped, from an error's text, so a
// dial or read failure that quotes the request URL cannot leak it.
func (f *Finnhub) redact(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if f.key == "" {
		return s
	}
	s = strings.ReplaceAll(s, url.QueryEscape(f.key), "<redacted>")
	return strings.ReplaceAll(s, f.key, "<redacted>")
}

// jitter spreads a retry over [d/2, d) so replicas do not reconnect in step.
func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// bar is one symbol's trades for one second.
type bar struct {
	symbol                string
	second                time.Time
	open, high, low, last float64
	volume                float64
	firstAt, lastAt       time.Time // trade times of open and close
}

type barKey struct {
	symbol string
	second time.Time
}

// coalescer folds trades into one-second bars by the trade's own timestamp.
// It is pure: callers supply the clock to flush, so it is tested without
// sleeping. Open is the earliest trade in the second and close the latest,
// by trade time (arrival order breaks ties).
type coalescer struct {
	mu      sync.Mutex
	open    map[barKey]*bar
	emitted map[string]time.Time // newest second emitted per symbol
}

func newCoalescer() *coalescer {
	return &coalescer{open: make(map[barKey]*bar), emitted: make(map[string]time.Time)}
}

// add records a trade. It reports false, and records nothing, when the
// trade's second was already emitted: reopening it would write a second bar
// for the same second.
func (c *coalescer) add(symbol string, price, volume float64, at time.Time) bool {
	second := at.Truncate(barWidth)

	c.mu.Lock()
	defer c.mu.Unlock()
	if done, ok := c.emitted[symbol]; ok && !second.After(done) {
		return false
	}
	k := barKey{symbol, second}
	b, ok := c.open[k]
	if !ok {
		c.open[k] = &bar{symbol: symbol, second: second, open: price, high: price, low: price, last: price, volume: volume, firstAt: at, lastAt: at}
		return true
	}
	if at.Before(b.firstAt) {
		b.open, b.firstAt = price, at
	}
	if !at.Before(b.lastAt) {
		b.last, b.lastAt = price, at
	}
	b.high = max(b.high, price)
	b.low = min(b.low, price)
	b.volume += volume
	return true
}

// flush removes and returns every bar whose second ended at least
// coalesceGrace before now, oldest first.
func (c *coalescer) flush(now time.Time) []bar {
	c.mu.Lock()
	defer c.mu.Unlock()

	var due []bar
	for k, b := range c.open {
		if b.second.Add(barWidth + coalesceGrace).After(now) {
			continue
		}
		due = append(due, *b)
		delete(c.open, k)
		if k.second.After(c.emitted[k.symbol]) {
			c.emitted[k.symbol] = k.second
		}
	}
	slices.SortFunc(due, func(a, b bar) int {
		if d := a.second.Compare(b.second); d != 0 {
			return d
		}
		return strings.Compare(a.symbol, b.symbol)
	})
	return due
}
