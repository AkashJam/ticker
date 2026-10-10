package sweep

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/source"
)

var ny = mustLoc("America/New_York")

func mustLoc(name string) *time.Location {
	l, err := time.LoadLocation(name)
	if err != nil {
		panic(err)
	}
	return l
}

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fridayClose is the closing print of Fri 2026-10-09 (16:00 EDT).
var fridayClose = time.Date(2026, 10, 9, 20, 0, 0, 0, time.UTC)

func quoteAt(at time.Time) Quote {
	return Quote{Open: 100, High: 110, Low: 95, Current: 105, PrevClose: 99, At: at}
}

type fakeQuoter struct {
	mu     sync.Mutex
	def    Quote
	quotes map[string]Quote
	errs   map[string][]error // queued errors, consumed one per call
	calls  map[string]int
}

func newFakeQuoter(def Quote) *fakeQuoter {
	return &fakeQuoter{def: def, quotes: map[string]Quote{}, errs: map[string][]error{}, calls: map[string]int{}}
}

func (f *fakeQuoter) Quote(_ context.Context, symbol string) (Quote, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[symbol]++
	if q := f.errs[symbol]; len(q) > 0 {
		f.errs[symbol] = q[1:]
		return Quote{}, q[0]
	}
	if q, ok := f.quotes[symbol]; ok {
		return q, nil
	}
	return f.def, nil
}

func (f *fakeQuoter) total() int {
	n := 0
	for _, c := range f.calls {
		n += c
	}
	return n
}

type fakeStore struct {
	bars    map[string]source.Candle // symbol|interval|time
	upserts int
}

func newFakeStore() *fakeStore { return &fakeStore{bars: map[string]source.Candle{}} }

func key(symbol, interval string, at time.Time) string {
	return symbol + "|" + interval + "|" + at.String()
}

func (s *fakeStore) UpsertCandle(_ context.Context, c source.Candle) error {
	s.upserts++
	s.bars[key(c.Symbol, c.Interval, c.Time)] = c
	return nil
}

func (s *fakeStore) HasCandle(_ context.Context, symbol, interval string, at time.Time) (bool, error) {
	_, ok := s.bars[key(symbol, interval, at)]
	return ok, nil
}

type harness struct {
	r      *Runner
	q      *fakeQuoter
	st     *fakeStore
	sleeps []time.Duration
}

func newHarness(symbols []string, def Quote, now time.Time) *harness {
	h := &harness{q: newFakeQuoter(def), st: newFakeStore()}
	h.r = NewRunner(h.q, h.st, symbols, ny, discardLog())
	h.r.Now = func() time.Time { return now }
	h.r.Sleep = func(_ context.Context, d time.Duration) error { h.sleeps = append(h.sleeps, d); return nil }
	return h
}

// fridayEvening is just after the 16:15 fire on the Friday above.
var fridayEvening = time.Date(2026, 10, 9, 20, 16, 0, 0, time.UTC)

func TestRun_WritesEverySymbolOHLCOnlyAtOneCallPerSecond(t *testing.T) {
	symbols := source.SweepSymbolCodes()
	h := newHarness(symbols, quoteAt(fridayClose), fridayEvening)

	res, err := h.r.Run(context.Background(), ModeScheduled)
	if err != nil {
		t.Fatal(err)
	}
	if res.State != StateWritten || res.Written != 28 || len(res.Failed) != 0 {
		t.Fatalf("result = %+v", res)
	}

	session := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	for _, s := range symbols {
		c, ok := h.st.bars[key(s, "1d", session)]
		if !ok {
			t.Fatalf("no 1d bar for %s at %s", s, session)
		}
		if c.Volume != nil {
			t.Errorf("%s: volume = %v, want nil (/quote carries none)", s, *c.Volume)
		}
		if c.Open != 100 || c.High != 110 || c.Low != 95 || c.Close != 105 {
			t.Errorf("%s: bar = %+v", s, c)
		}
	}
	if h.q.total() != 28 {
		t.Errorf("made %d calls, want exactly 28 (the canary's quote is reused)", h.q.total())
	}
	if len(h.sleeps) != 27 {
		t.Fatalf("slept %d times, want 27 (between 28 calls)", len(h.sleeps))
	}
	for _, d := range h.sleeps {
		if d != time.Second {
			t.Errorf("sleep = %s, want 1s", d)
		}
	}
}

func TestRun_StampsTheSessionDateNotTheFetchTime(t *testing.T) {
	// Fetched on Saturday morning; the bar still belongs to Friday.
	h := newHarness([]string{"RY"}, quoteAt(fridayClose), time.Date(2026, 10, 10, 13, 0, 0, 0, time.UTC))
	if _, err := h.r.Run(context.Background(), ModeManual); err != nil {
		t.Fatal(err)
	}
	if _, ok := h.st.bars[key("RY", "1d", time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC))]; !ok {
		t.Errorf("bar not stamped with the Friday session date: %v", h.st.bars)
	}
}

func TestRun_HolidayWritesNothingAndCostsOneCall(t *testing.T) {
	// Fri 2026-10-09 is a normal day here; pretend the exchange was shut so
	// /quote still answers with Thursday's close.
	thursdayClose := time.Date(2026, 10, 8, 20, 0, 0, 0, time.UTC)
	h := newHarness(source.SweepSymbolCodes(), quoteAt(thursdayClose), fridayEvening)

	res, err := h.r.Run(context.Background(), ModeScheduled)
	if err != nil {
		t.Fatal(err)
	}
	if res.State != StateHoliday || h.st.upserts != 0 || h.q.total() != 1 {
		t.Errorf("state=%s upserts=%d calls=%d, want holiday/0/1", res.State, h.st.upserts, h.q.total())
	}
}

func TestRun_SessionStillOpenIsNotFinal(t *testing.T) {
	midSession := time.Date(2026, 10, 9, 15, 0, 0, 0, time.UTC) // 11:00 EDT
	h := newHarness([]string{"RY", "BAP"}, quoteAt(midSession), midSession.Add(5*time.Minute))

	res, err := h.r.Run(context.Background(), ModeManual)
	if err != nil {
		t.Fatal(err)
	}
	if res.State != StateNotFinal || h.st.upserts != 0 {
		t.Errorf("state=%s upserts=%d, want not-final with nothing written", res.State, h.st.upserts)
	}
}

func TestRun_OneFailureLeavesTheRestWrittenAndIsNamed(t *testing.T) {
	h := newHarness([]string{"RY", "KB", "BAP"}, quoteAt(fridayClose), fridayEvening)
	h.q.errs["KB"] = []error{&StatusError{Code: 403}}

	res, err := h.r.Run(context.Background(), ModeScheduled)
	if err != nil {
		t.Fatal(err)
	}
	if res.State != StateFailed || res.Written != 2 || len(res.Failed) != 1 || res.Failed[0] != "KB" {
		t.Errorf("result = %+v", res)
	}
	if h.q.calls["KB"] != 1 {
		t.Errorf("KB called %d times; a 403 must not be retried", h.q.calls["KB"])
	}
}

func TestRun_RetriesARateLimitAndHonoursRetryAfter(t *testing.T) {
	h := newHarness([]string{"RY", "BAP"}, quoteAt(fridayClose), fridayEvening)
	h.q.errs["BAP"] = []error{&StatusError{Code: 429, RetryAfter: 9 * time.Second}}

	res, err := h.r.Run(context.Background(), ModeScheduled)
	if err != nil {
		t.Fatal(err)
	}
	if res.State != StateWritten || res.Written != 2 {
		t.Fatalf("result = %+v", res)
	}
	if h.q.calls["BAP"] != 2 {
		t.Errorf("BAP called %d times, want 2", h.q.calls["BAP"])
	}
	var waited9 bool
	for _, d := range h.sleeps {
		waited9 = waited9 || d == 9*time.Second
	}
	if !waited9 {
		t.Errorf("sleeps %v never waited the server's 9s", h.sleeps)
	}
}

func TestRun_GivesUpAfterAttempts(t *testing.T) {
	h := newHarness([]string{"RY", "BAP"}, quoteAt(fridayClose), fridayEvening)
	boom := errors.New("network down")
	h.q.errs["BAP"] = []error{boom, boom, boom, boom}

	res, _ := h.r.Run(context.Background(), ModeScheduled)
	if h.q.calls["BAP"] != 3 || len(res.Failed) != 1 {
		t.Errorf("calls=%d failed=%v, want 3 attempts then a recorded failure", h.q.calls["BAP"], res.Failed)
	}
}

func TestRun_UnknownSymbolIsNotRetried(t *testing.T) {
	h := newHarness([]string{"RY", "NOPE"}, quoteAt(fridayClose), fridayEvening)
	h.q.errs["NOPE"] = []error{ErrNoData}

	res, _ := h.r.Run(context.Background(), ModeScheduled)
	if h.q.calls["NOPE"] != 1 || len(res.Failed) != 1 {
		t.Errorf("calls=%d failed=%v", h.q.calls["NOPE"], res.Failed)
	}
}

func TestRun_AStaleQuoteIsRejectedNotWrittenAsToday(t *testing.T) {
	h := newHarness([]string{"RY", "BAP"}, quoteAt(fridayClose), fridayEvening)
	h.q.quotes["BAP"] = quoteAt(time.Date(2026, 10, 8, 20, 0, 0, 0, time.UTC)) // halted: still Thursday

	res, _ := h.r.Run(context.Background(), ModeScheduled)
	if len(res.Failed) != 1 || res.Failed[0] != "BAP" || res.Written != 1 {
		t.Errorf("result = %+v", res)
	}
}

func TestRun_InvalidQuoteIsRejected(t *testing.T) {
	bad := quoteAt(fridayClose)
	bad.Open = 0
	h := newHarness([]string{"RY"}, bad, fridayEvening)

	res, _ := h.r.Run(context.Background(), ModeScheduled)
	if len(res.Failed) != 1 || h.st.upserts != 0 {
		t.Errorf("result = %+v upserts=%d", res, h.st.upserts)
	}
}

func TestRun_CanaryFailureStopsAfterOneSymbol(t *testing.T) {
	h := newHarness(source.SweepSymbolCodes(), quoteAt(fridayClose), fridayEvening)
	h.q.errs["RY"] = []error{&StatusError{Code: 401}}

	res, err := h.r.Run(context.Background(), ModeScheduled)
	if err != nil {
		t.Fatal(err)
	}
	if res.State != StateFailed || len(res.Failed) != 1 || res.Failed[0] != "RY" || h.q.total() != 1 {
		t.Errorf("result = %+v calls=%d", res, h.q.total())
	}
}

func TestRun_CatchUpWritesAMissedSessionOverTheWeekend(t *testing.T) {
	saturday := time.Date(2026, 10, 10, 13, 0, 0, 0, time.UTC)
	h := newHarness([]string{"RY", "BAP"}, quoteAt(fridayClose), saturday)

	res, err := h.r.Run(context.Background(), ModeCatchUp)
	if err != nil {
		t.Fatal(err)
	}
	if res.State != StateWritten || res.Written != 2 {
		t.Errorf("result = %+v", res)
	}
}

func TestRun_CatchUpDoesNothingWhenTheCanaryHasItsBar(t *testing.T) {
	h := newHarness([]string{"RY", "BAP"}, quoteAt(fridayClose), fridayEvening)
	h.st.bars[key("RY", "1d", time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC))] = source.Candle{}

	res, err := h.r.Run(context.Background(), ModeCatchUp)
	if err != nil {
		t.Fatal(err)
	}
	if res.State != StateUpToDate || h.st.upserts != 0 || h.q.total() != 1 {
		t.Errorf("state=%s upserts=%d calls=%d", res.State, h.st.upserts, h.q.total())
	}
}

func TestRun_ManualRewritesAnExistingBar(t *testing.T) {
	h := newHarness([]string{"RY"}, quoteAt(fridayClose), fridayEvening)
	h.st.bars[key("RY", "1d", time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC))] = source.Candle{}

	if _, err := h.r.Run(context.Background(), ModeManual); err != nil {
		t.Fatal(err)
	}
	if h.st.upserts != 1 {
		t.Errorf("upserts = %d, want 1", h.st.upserts)
	}
}

func TestRun_StopsWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	h := newHarness([]string{"RY", "BAP", "KB"}, quoteAt(fridayClose), fridayEvening)
	h.r.Sleep = func(ctx context.Context, _ time.Duration) error { cancel(); return ctx.Err() }

	if _, err := h.r.Run(ctx, ModeScheduled); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}
