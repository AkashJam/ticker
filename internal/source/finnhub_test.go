package source

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const testKey = "k3y-with+chars/and=more"

func quietLog() (*slog.Logger, *strings.Builder) {
	var sb strings.Builder
	return slog.New(slog.NewTextHandler(&lockedWriter{w: &sb}, &slog.HandlerOptions{Level: slog.LevelDebug})), &sb
}

type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func newTestFinnhub(url string, log *slog.Logger) *Finnhub {
	f := NewFinnhub(testKey, StreamUniverse(), log)
	f.wsURL = url
	return f
}

func tradeFrame(symbol string, at time.Time, price, vol float64) string {
	return fmt.Sprintf(`{"type":"trade","data":[{"s":%q,"p":%v,"v":%v,"t":%d,"c":["1"]}]}`, symbol, price, vol, at.UnixMilli())
}

// feed is a fake Finnhub: it checks auth, records subscriptions per
// connection, then runs script against each accepted connection.
type feed struct {
	t       *testing.T
	conns   atomic.Int32
	subs    chan string
	script  func(ctx context.Context, n int, c *websocket.Conn)
	badAuth atomic.Bool
}

func (fd *feed) handler(w http.ResponseWriter, r *http.Request) {
	if fd.badAuth.Load() || r.URL.Query().Get("token") != testKey {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	n := int(fd.conns.Add(1))
	ctx := r.Context()
	go func() {
		for {
			_, b, err := c.Read(ctx)
			if err != nil {
				return
			}
			select {
			case fd.subs <- string(b):
			default:
			}
		}
	}()
	fd.script(ctx, n, c)
}

func startFeed(t *testing.T, script func(ctx context.Context, n int, c *websocket.Conn)) (*feed, *httptest.Server) {
	t.Helper()
	fd := &feed{t: t, subs: make(chan string, 64), script: script}
	srv := httptest.NewServer(http.HandlerFunc(fd.handler))
	t.Cleanup(srv.Close)
	return fd, srv
}

func collect(t *testing.T, ch <-chan NormalizedTick, n int, within time.Duration) []NormalizedTick {
	t.Helper()
	var out []NormalizedTick
	deadline := time.After(within)
	for len(out) < n {
		select {
		case tk, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed after %d of %d ticks", len(out), n)
			}
			out = append(out, tk)
		case <-deadline:
			t.Fatalf("got %d of %d ticks within %s", len(out), n, within)
		}
	}
	return out
}

func TestFinnhub_StreamsBarsAndAuthsByQueryToken(t *testing.T) {
	past := time.Now().Add(-5 * time.Second).Truncate(time.Second)
	fd, srv := startFeed(t, func(ctx context.Context, _ int, c *websocket.Conn) {
		for _, f := range []string{
			`{"type":"ping"}`,
			tradeFrame("AAPL", past.Add(100*time.Millisecond), 100, 10),
			tradeFrame("AAPL", past.Add(200*time.Millisecond), 103, 5),
			tradeFrame("AAPL", past.Add(300*time.Millisecond), 99, 5),
			tradeFrame("AAPL", past.Add(900*time.Millisecond), 101, 20),
			`{"type":"unknown"}`,
		} {
			_ = c.Write(ctx, websocket.MessageText, []byte(f))
		}
		<-ctx.Done()
	})
	log, _ := quietLog()
	f := newTestFinnhub(srv.URL, log)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := f.Subscribe(ctx, []string{"AAPL", "NVDA"})
	if err != nil {
		t.Fatal(err)
	}
	got := collect(t, ch, 1, 5*time.Second)[0]

	if got.Symbol != "AAPL" || got.Open != 100 || got.High != 103 || got.Low != 99 || got.Price != 101 || got.Volume != 40 {
		t.Errorf("bar = %+v, want AAPL O100 H103 L99 C101 V40", got)
	}
	if !got.Timestamp.Equal(past) {
		t.Errorf("timestamp = %s, want the bar's second %s", got.Timestamp, past)
	}
	if got.Simulated || got.AssetType != AssetTypeStock {
		t.Errorf("simulated=%v asset=%v, want a real stock", got.Simulated, got.AssetType)
	}

	var subs []string
	for len(subs) < 2 {
		select {
		case s := <-fd.subs:
			subs = append(subs, s)
		case <-time.After(2 * time.Second):
			t.Fatalf("subscriptions seen: %v", subs)
		}
	}
	if !strings.Contains(subs[0], `"subscribe"`) || !strings.Contains(subs[0], "AAPL") || !strings.Contains(subs[1], "NVDA") {
		t.Errorf("subscriptions = %v", subs)
	}
}

func TestFinnhub_ReconnectsAndResubscribes(t *testing.T) {
	past := time.Now().Add(-5 * time.Second).Truncate(time.Second)
	fd, srv := startFeed(t, func(ctx context.Context, n int, c *websocket.Conn) {
		if n == 1 {
			time.Sleep(150 * time.Millisecond) // let the subscribe arrive, then drop
			_ = c.Close(websocket.StatusGoingAway, "bye")
			return
		}
		_ = c.Write(ctx, websocket.MessageText, []byte(tradeFrame("JPM", past, 200, 7)))
		<-ctx.Done()
	})
	log, _ := quietLog()
	f := newTestFinnhub(srv.URL, log)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := f.Subscribe(ctx, []string{"JPM"})
	if err != nil {
		t.Fatal(err)
	}
	got := collect(t, ch, 1, 10*time.Second)[0]
	if got.Symbol != "JPM" || got.Price != 200 {
		t.Errorf("bar after reconnect = %+v", got)
	}
	if fd.conns.Load() < 2 {
		t.Errorf("connections = %d, want a reconnect", fd.conns.Load())
	}
	// A subscribe on each connection.
	n := 0
loop:
	for {
		select {
		case <-fd.subs:
			n++
		case <-time.After(300 * time.Millisecond):
			break loop
		}
	}
	if n < 2 {
		t.Errorf("subscribe messages = %d, want one per connection", n)
	}
}

func TestFinnhub_RejectedKeyFailsSubscribe(t *testing.T) {
	fd, srv := startFeed(t, func(ctx context.Context, _ int, _ *websocket.Conn) { <-ctx.Done() })
	fd.badAuth.Store(true)
	log, logs := quietLog()
	f := newTestFinnhub(srv.URL, log)

	_, err := f.Subscribe(context.Background(), []string{"AAPL"})
	if err == nil || err.Error() != ErrUnauthorized.Error() {
		t.Fatalf("Subscribe error = %v, want ErrUnauthorized", err)
	}
	if strings.Contains(err.Error(), testKey) || strings.Contains(logs.String(), testKey) {
		t.Errorf("the key leaked into an error or log")
	}
	if fd.conns.Load() != 0 {
		t.Errorf("connections = %d, want none", fd.conns.Load())
	}
}

func TestFinnhub_KeyRevokedMidRunStopsAndClosesChannel(t *testing.T) {
	// The first connection is accepted, then dropped; the key is revoked
	// before the redial, which is therefore refused.
	drop := make(chan struct{})
	fd, srv := startFeed(t, func(ctx context.Context, _ int, c *websocket.Conn) {
		select {
		case <-drop:
			_ = c.Close(websocket.StatusGoingAway, "bye")
		case <-ctx.Done():
		}
	})
	log, logs := quietLog()
	f := newTestFinnhub(srv.URL, log)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := f.Subscribe(ctx, []string{"AAPL"})
	if err != nil {
		t.Fatal(err)
	}
	fd.badAuth.Store(true)
	close(drop) // the server drops the live connection, so the source redials

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("unexpected tick")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("channel did not close after the key was rejected")
	}
	if strings.Contains(logs.String(), testKey) {
		t.Errorf("the key leaked into a log: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "key rejected") {
		t.Errorf("expected one error log about the rejected key, got: %s", logs.String())
	}
}

func TestFinnhub_ContextCancelClosesChannel(t *testing.T) {
	_, srv := startFeed(t, func(ctx context.Context, _ int, _ *websocket.Conn) { <-ctx.Done() })
	log, _ := quietLog()
	f := newTestFinnhub(srv.URL, log)
	ctx, cancel := context.WithCancel(context.Background())

	ch, err := f.Subscribe(ctx, []string{"AAPL"})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("unexpected tick")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("channel did not close after cancel")
	}
}

func TestFinnhub_RedactStripsKeyRawAndEscaped(t *testing.T) {
	log, _ := quietLog()
	f := newTestFinnhub("ws://x", log)
	err := fmt.Errorf(`Get "wss://ws.finnhub.io?token=k3y-with%%2Bchars%%2Fand%%3Dmore": dial tcp: no such host; raw %s`, testKey)
	got := f.redact(err)
	if strings.Contains(got, testKey) || strings.Contains(got, "k3y-with") {
		t.Errorf("redact left the key in: %s", got)
	}
}

func TestFinnhub_BackfillIsEmptyAndNameIsFinnhub(t *testing.T) {
	log, _ := quietLog()
	f := newTestFinnhub("ws://x", log)
	c, err := f.Backfill(context.Background(), "AAPL", time.Now().Add(-time.Hour), time.Now())
	if err != nil || len(c) != 0 {
		t.Errorf("Backfill = %v, %v; want empty", c, err)
	}
	if f.Name() != "finnhub" {
		t.Errorf("Name = %q", f.Name())
	}
}

func TestJitterStaysInRange(t *testing.T) {
	for range 200 {
		d := jitter(8 * time.Second)
		if d < 4*time.Second || d > 8*time.Second {
			t.Fatalf("jitter = %s, want within [4s, 8s]", d)
		}
	}
}

// --- coalescer ---

func at(base time.Time, ms int) time.Time { return base.Add(time.Duration(ms) * time.Millisecond) }

func TestCoalescer_OHLCVFromOutOfOrderTrades(t *testing.T) {
	base := time.Date(2026, 10, 13, 15, 0, 0, 0, time.UTC)
	c := newCoalescer()
	// Arrives out of order: the 300ms trade is first on the wire.
	c.add("AAPL", 101, 5, at(base, 300))
	c.add("AAPL", 100, 10, at(base, 100)) // earliest -> open
	c.add("AAPL", 104, 2, at(base, 500))  // highest
	c.add("AAPL", 98, 3, at(base, 900))   // lowest and latest -> close

	bars := c.flush(base.Add(barWidth + coalesceGrace))
	if len(bars) != 1 {
		t.Fatalf("bars = %d, want 1", len(bars))
	}
	b := bars[0]
	if b.open != 100 || b.high != 104 || b.low != 98 || b.last != 98 || b.volume != 20 || !b.second.Equal(base) {
		t.Errorf("bar = %+v", b)
	}
}

func TestCoalescer_EmitsOnlyAfterGraceAndOnlyOnce(t *testing.T) {
	base := time.Date(2026, 10, 13, 15, 0, 0, 0, time.UTC)
	c := newCoalescer()
	c.add("AAPL", 100, 1, at(base, 10))

	if got := c.flush(base.Add(barWidth + coalesceGrace - time.Millisecond)); len(got) != 0 {
		t.Fatalf("flushed %d bars before the grace elapsed", len(got))
	}
	if got := c.flush(base.Add(barWidth + coalesceGrace)); len(got) != 1 {
		t.Fatalf("flushed %d bars at second+grace, want 1", len(got))
	}
	if got := c.flush(base.Add(time.Hour)); len(got) != 0 {
		t.Errorf("a bar was emitted twice")
	}
}

func TestCoalescer_EmptySecondsEmitNothing(t *testing.T) {
	base := time.Date(2026, 10, 13, 15, 0, 0, 0, time.UTC)
	c := newCoalescer()
	c.add("AAPL", 100, 1, at(base, 10))
	c.add("AAPL", 101, 1, at(base, 3010)) // seconds 1 and 2 are empty

	bars := c.flush(base.Add(time.Minute))
	if len(bars) != 2 || !bars[0].second.Equal(base) || !bars[1].second.Equal(base.Add(3*time.Second)) {
		t.Errorf("bars = %+v, want only the two seconds that had trades", bars)
	}
}

func TestCoalescer_LateTradeAfterEmissionIsDropped(t *testing.T) {
	base := time.Date(2026, 10, 13, 15, 0, 0, 0, time.UTC)
	c := newCoalescer()
	c.add("AAPL", 100, 1, at(base, 10))
	c.flush(base.Add(time.Minute))

	if c.add("AAPL", 50, 999, at(base, 500)) {
		t.Errorf("a trade for an emitted second was accepted")
	}
	if got := c.flush(base.Add(time.Hour)); len(got) != 0 {
		t.Errorf("a dropped trade produced a bar: %+v", got)
	}
	// A later second is unaffected.
	if !c.add("AAPL", 101, 1, at(base, 5000)) {
		t.Errorf("a trade for a later second was rejected")
	}
}

func TestCoalescer_SymbolsAreIndependent(t *testing.T) {
	base := time.Date(2026, 10, 13, 15, 0, 0, 0, time.UTC)
	c := newCoalescer()
	c.add("AAPL", 100, 1, at(base, 10))
	c.add("NVDA", 200, 2, at(base, 20))
	c.flush(base.Add(time.Minute))

	// AAPL's second is emitted, NVDA's is too; each symbol tracks its own.
	if !c.add("AAPL", 101, 1, at(base, 2000)) || !c.add("NVDA", 201, 1, at(base, 2000)) {
		t.Errorf("later seconds were rejected")
	}
	if c.add("NVDA", 5, 1, at(base, 100)) {
		t.Errorf("NVDA's emitted second accepted a late trade")
	}
}

func TestStreamUniverseIsTheEighteenStocks(t *testing.T) {
	u := StreamUniverse()
	if len(u) != 18 {
		t.Fatalf("StreamUniverse has %d symbols, want 18 (13 country + 5 global)", len(u))
	}
	for _, s := range u {
		if s.Type != AssetTypeStock {
			t.Errorf("%s is %s: ETFs are daily-close only and must not stream", s.Symbol, s.Type)
		}
	}
	if got := StreamSymbolCodes(); len(got) != 18 || got[0] != "RY" {
		t.Errorf("StreamSymbolCodes = %v", got)
	}
}

// pingsOnly is a socket that stays up and answers, and never sends a trade: the
// failure of 2026-10-16, after a reconnect at 08:30 ET.
func pingsOnly(ctx context.Context, _ int, c *websocket.Conn) {
	for ctx.Err() == nil {
		if c.Write(ctx, websocket.MessageText, []byte(`{"type":"ping"}`)) != nil {
			return
		}
		time.Sleep(40 * time.Millisecond)
	}
}

// quietFinnhub is a source whose socket must be silent for 300ms to count as dead.
func quietFinnhub(t *testing.T, script func(ctx context.Context, n int, c *websocket.Conn), session SessionFunc) (*feed, context.CancelFunc) {
	t.Helper()
	fd, srv := startFeed(t, script)
	log, _ := quietLog()
	f := newTestFinnhub(srv.URL, log).WithSession(session)
	f.idle = 300 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := f.Subscribe(ctx, []string{"AAPL"}); err != nil {
		t.Fatal(err)
	}
	return fd, cancel
}

func TestFinnhub_ReplacesASocketThatPingsButSendsNoTradesWhileTheMarketIsOpen(t *testing.T) {
	open := func(context.Context, time.Time) (bool, time.Time) { return true, time.Time{} }
	fd, cancel := quietFinnhub(t, pingsOnly, open)
	defer cancel()

	deadline := time.After(10 * time.Second)
	for fd.conns.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("connections = %d: a silent socket was never replaced", fd.conns.Load())
		case <-time.After(50 * time.Millisecond):
		}
	}
	// And it resubscribes on the new connection, as after any drop.
	n := 0
loop:
	for {
		select {
		case <-fd.subs:
			n++
		case <-time.After(300 * time.Millisecond):
			break loop
		}
	}
	if n < 2 {
		t.Errorf("subscribe messages = %d, want one per connection", n)
	}
}

func TestFinnhub_LeavesAQuietSocketAloneWhenTheMarketIsClosed(t *testing.T) {
	closed := func(context.Context, time.Time) (bool, time.Time) { return false, time.Time{} }
	fd, cancel := quietFinnhub(t, pingsOnly, closed)
	defer cancel()
	time.Sleep(2500 * time.Millisecond) // well past the idle time and the reconnect backoff
	if got := fd.conns.Load(); got != 1 {
		t.Errorf("connections = %d, want 1: silence overnight is not a fault", got)
	}
}

func TestFinnhub_DoesNotJudgeTheFirstMomentsOfASession(t *testing.T) {
	// Open "since now" on every call: the session has only just begun, so a socket
	// that has been quiet since the night is not yet a dead one.
	justOpened := func(_ context.Context, now time.Time) (bool, time.Time) { return true, now }
	fd, cancel := quietFinnhub(t, pingsOnly, justOpened)
	defer cancel()
	time.Sleep(2500 * time.Millisecond)
	if got := fd.conns.Load(); got != 1 {
		t.Errorf("connections = %d, want 1: the open must not be judged by the night before", got)
	}
}

func TestFinnhub_KeepsASocketThatTrades(t *testing.T) {
	open := func(context.Context, time.Time) (bool, time.Time) { return true, time.Time{} }
	trading := func(ctx context.Context, _ int, c *websocket.Conn) {
		for ctx.Err() == nil {
			if c.Write(ctx, websocket.MessageText, []byte(tradeFrame("AAPL", time.Now(), 100, 1))) != nil {
				return
			}
			time.Sleep(40 * time.Millisecond)
		}
	}
	fd, cancel := quietFinnhub(t, trading, open)
	defer cancel()
	time.Sleep(2500 * time.Millisecond)
	if got := fd.conns.Load(); got != 1 {
		t.Errorf("connections = %d, want 1: a socket that trades is healthy", got)
	}
}

func TestFinnhub_NoSessionFuncMeansNoQuietCheck(t *testing.T) {
	fd, srv := startFeed(t, pingsOnly)
	log, _ := quietLog()
	f := newTestFinnhub(srv.URL, log)
	f.idle = 300 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := f.Subscribe(ctx, []string{"AAPL"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond)
	if got := fd.conns.Load(); got != 1 {
		t.Errorf("connections = %d, want 1: dev and tests model no market", got)
	}
}
