package fixture

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/AkashJam/ticker/internal/coverage"
	"github.com/AkashJam/ticker/internal/ecb"
	"github.com/AkashJam/ticker/internal/news"
	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/trigger"
)

// fakeStore is Timescale in memory: daily closes for the 28 symbols over the
// backfill's weekdays, and an events table with the store's semantics.
type fakeStore struct {
	bars   map[string][]source.Candle
	events map[string]trigger.Trip
	now    time.Time
}

func key(symbol string, session time.Time) string {
	return symbol + " " + session.Format(time.DateOnly)
}

// newFakeStore builds a seeded walk per symbol, ending on last.
func newFakeStore(last time.Time) *fakeStore {
	var days []time.Time
	for d := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC); !d.After(last); d = d.AddDate(0, 0, 1) {
		if d.Weekday() != time.Saturday && d.Weekday() != time.Sunday {
			days = append(days, d)
		}
	}
	f := &fakeStore{bars: map[string][]source.Candle{}, events: map[string]trigger.Trip{}, now: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)}
	for i, sym := range source.SweepSymbolCodes() {
		seed := uint32(i*7919 + 17)
		px := 50.0 + float64(i)
		for _, d := range days {
			seed = seed*1664525 + 1013904223
			px *= 1 + (float64(seed)/math.MaxUint32-0.5)*0.03
			f.bars[sym] = append(f.bars[sym], source.Candle{Time: d, Symbol: sym, Interval: trigger.DailyInterval, Close: px})
		}
	}
	return f
}

func (f *fakeStore) Candles(_ context.Context, symbol, _ string, from, to time.Time) ([]source.Candle, error) {
	var out []source.Candle
	for _, b := range f.bars[symbol] {
		if (from.IsZero() || !b.Time.Before(from)) && (to.IsZero() || b.Time.Before(to)) {
			out = append(out, b)
		}
	}
	return out, nil
}

func (f *fakeStore) LatestCandle(_ context.Context, symbol, _ string) (source.Candle, bool, error) {
	b := f.bars[symbol]
	if len(b) == 0 {
		return source.Candle{}, false, nil
	}
	return b[len(b)-1], true, nil
}

func (f *fakeStore) InsertEvent(_ context.Context, t trigger.Trip) (bool, error) {
	if _, ok := f.events[key(t.Symbol, t.SessionDate)]; ok {
		return false, nil
	}
	f.events[key(t.Symbol, t.SessionDate)] = t
	return true, nil
}

func (f *fakeStore) Event(_ context.Context, symbol string, session time.Time) (trigger.Trip, bool, error) {
	t, ok := f.events[key(symbol, session)]
	return t, ok, nil
}

func (f *fakeStore) SetSplit(_ context.Context, symbol string, session time.Time, label *string, split []byte) (bool, error) {
	t, ok := f.events[key(symbol, session)]
	if !ok || t.Split != nil {
		return false, nil
	}
	t.SplitLabel, t.Split = label, split
	f.events[key(symbol, session)] = t
	return true, nil
}

func (f *fakeStore) SetCoverage(_ context.Context, symbol string, session time.Time, cov []byte) error {
	t := f.events[key(symbol, session)]
	now := f.now
	t.Coverage, t.CoverageFetchedAt = cov, &now
	f.events[key(symbol, session)] = t
	return nil
}

// PendingCoverage mirrors the store: the fourteen days before, coverage missing
// or not complete, oldest first.
func (f *fakeStore) PendingCoverage(_ context.Context, before time.Time) ([]trigger.Trip, error) {
	var out []trigger.Trip
	for _, t := range f.events {
		if !t.SessionDate.Before(before) || t.SessionDate.Before(before.AddDate(0, 0, -14)) {
			continue
		}
		if c, _ := coverage.Decode(t.Coverage); c != nil && c.Complete {
			continue
		}
		out = append(out, t)
	}
	slices.SortFunc(out, func(a, b trigger.Trip) int { return strings.Compare(key(a.Symbol, a.SessionDate), key(b.Symbol, b.SessionDate)) })
	return out, nil
}

// syntheticState is an ECB window for the picked sensors' currencies, carried
// forward daily, ending after every judged session.
func syntheticState(t *testing.T) fstest.MapFS {
	t.Helper()
	start := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	last := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	n := int(last.Sub(start).Hours()/24) + 1
	st := ecb.State{Last: last, Wanted: []string{"BRL", "JPY", "KRW", "USD"}, Window: map[string]*ecb.DailyWindow{}}
	for i, code := range st.Wanted {
		base := map[string]float64{"BRL": 6.1, "JPY": 170, "KRW": 1600, "USD": 1.16}[code]
		rates := make([]float64, n)
		seed := uint32(i + 3)
		for d := range rates {
			seed = seed*1664525 + 1013904223
			rates[d] = base * (1 + (float64(seed)/math.MaxUint32-0.5)*0.01)
		}
		st.Window[code] = &ecb.DailyWindow{Start: start, Rates: rates}
	}
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	return fstest.MapFS{StateFile: {Data: b}}
}

func newYork(t *testing.T) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

func deps(t *testing.T, st *fakeStore, env string) Deps {
	t.Helper()
	return Deps{
		Store: st, Loc: newYork(t), Env: env, SplitThreshold: 2,
		Thin: func(context.Context) (map[string]bool, error) { return map[string]bool{"KB": true}, nil },
		Log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func withState(t *testing.T) {
	t.Helper()
	prev := stateFS
	stateFS = syntheticState(t)
	t.Cleanup(func() { stateFS = prev })
}

func sessionsBack(st *fakeStore, back int) time.Time {
	b := st.bars[source.SweepUniverse[0].Symbol]
	return b[len(b)-1-back].Time
}

func cov(t *testing.T, tr trigger.Trip) *coverage.Coverage {
	t.Helper()
	c, err := coverage.Decode(tr.Coverage)
	if err != nil || c == nil {
		t.Fatalf("%s: coverage %v, %v", tr.Symbol, c, err)
	}
	return c
}

func TestRefusesProduction(t *testing.T) {
	withState(t)
	_, err := Run(context.Background(), deps(t, newFakeStore(BackfillEnd), "prod"), StateTripped)
	if err == nil || !strings.Contains(err.Error(), "ENV=prod") {
		t.Fatalf("err = %v, want a refusal for ENV=prod", err)
	}
}

func TestRefusesADatabaseASweepHasWritten(t *testing.T) {
	withState(t)
	st := newFakeStore(BackfillEnd.AddDate(0, 0, 1)) // a Friday after the backfill
	_, err := Run(context.Background(), deps(t, st, "dev"), StateTripped)
	if err == nil || !strings.Contains(err.Error(), "a real sweep has run") {
		t.Fatalf("err = %v, want a refusal for a swept database", err)
	}
	if len(st.events) != 0 {
		t.Errorf("wrote %d events after refusing", len(st.events))
	}
}

func TestRefusesAnUnmigratedDatabase(t *testing.T) {
	withState(t)
	st := &fakeStore{bars: map[string][]source.Candle{}, events: map[string]trigger.Trip{}}
	if _, err := Run(context.Background(), deps(t, st, "dev"), StateTripped); err == nil || !strings.Contains(err.Error(), "ticker migrate") {
		t.Fatalf("err = %v, want a pointer to migrate", err)
	}
}

func TestRejectsAnUnknownState(t *testing.T) {
	if _, err := Run(context.Background(), deps(t, newFakeStore(BackfillEnd), "dev"), "empty"); err == nil {
		t.Fatal("ran an unknown state")
	}
}

func TestTrippedRecordsTheStory(t *testing.T) {
	withState(t)
	st := newFakeStore(BackfillEnd)
	sums, err := Run(context.Background(), deps(t, st, "dev"), StateTripped)
	if err != nil {
		t.Fatal(err)
	}
	if len(sums) != len(plans[StateTripped]) {
		t.Fatalf("%d runs, want %d", len(sums), len(plans[StateTripped]))
	}

	want := map[string]int{"CIB": 8, "CAT": 6, "ITUB": 4, "KB": 2, "MUFG": 0, "RY": 0}
	if len(st.events) != len(want) {
		var got []string
		for k := range st.events {
			got = append(got, k)
		}
		t.Fatalf("events %v, want exactly %v", got, want)
	}
	for sym, back := range want {
		ev, ok := st.events[key(sym, sessionsBack(st, back))]
		if !ok {
			t.Fatalf("no %s event %d sessions back", sym, back)
		}
		if ev.Threshold != Threshold || ev.WindowSessions != trigger.Window {
			t.Errorf("%s: threshold %v window %d, want %v and %d", sym, ev.Threshold, ev.WindowSessions, Threshold, trigger.Window)
		}
	}

	ry := st.events[key("RY", sessionsBack(st, 0))]
	if ry.Split != nil || ry.Coverage != nil {
		t.Errorf("RY: split %s coverage %s, want both pending", ry.Split, ry.Coverage)
	}

	cib := cov(t, st.events[key("CIB", sessionsBack(st, 8))])
	if bank := cib.Sources[coverage.SrcBank]; bank == nil || bank.Status != coverage.StatusError || bank.Attempts != coverage.MaxAttempts {
		t.Errorf("CIB bank: %+v, want an error frozen at %d attempts", bank, coverage.MaxAttempts)
	}
	var cibSplit trigger.SplitResult
	if err := json.Unmarshal(st.events[key("CIB", sessionsBack(st, 8))].Split, &cibSplit); err != nil || cibSplit.Reason != trigger.NoSplitNoECBCurrency {
		t.Errorf("CIB split %+v (%v), want none: %s", cibSplit, err, trigger.NoSplitNoECBCurrency)
	}

	cat := cov(t, st.events[key("CAT", sessionsBack(st, 6))])
	if cat.Routing != coverage.RoutingGlobal || !cat.Complete {
		t.Errorf("CAT: routing %s complete %v, want global and complete", cat.Routing, cat.Complete)
	}
	filings := cat.Sources[coverage.SrcFilings]
	if filings == nil || len(filings.Items) != 1 || len(filings.Items[0].ItemCodes) != 2 || filings.Items[0].Form != "8-K" {
		t.Errorf("CAT filings: %+v, want one 8-K with two item codes", filings)
	}
	if topic := cat.Sources[coverage.SrcTopic]; topic == nil || topic.Excluded != 2 || slices.ContainsFunc(topic.Outlets, func(o news.Outlet) bool { return o.Name == "MarketBeat" }) {
		t.Errorf("CAT topic: %+v, want MarketBeat's two items excluded and not listed", topic)
	}

	itub := cov(t, st.events[key("ITUB", sessionsBack(st, 4))])
	if d := itub.Sources[coverage.SrcDividend]; d == nil || d.Status != coverage.StatusError {
		t.Errorf("ITUB dividend: %+v, want an error", d)
	}

	kb := st.events[key("KB", sessionsBack(st, 2))]
	if !slices.Contains(kb.Caveats, "thin_listing") {
		t.Errorf("KB caveats %v, want thin_listing", kb.Caveats)
	}
	if d := cov(t, kb).Sources[coverage.SrcDividend]; d == nil || d.Dividend == nil || d.Dividend.Amount != 0.55 {
		t.Errorf("KB dividend: %+v, want the 0.55 ex-date", d)
	}

	mufg := cov(t, st.events[key("MUFG", sessionsBack(st, 0))])
	if mufg.Stage != trigger.StageTrip || mufg.Complete {
		t.Errorf("MUFG: stage %s complete %v, want the trip's fetch, not yet followed up", mufg.Stage, mufg.Complete)
	}

	for k := range st.events {
		if strings.HasPrefix(k, "NVDA ") {
			t.Errorf("NVDA has an event (%s); the record spec needs it to have none", k)
		}
	}
}

func TestQuietEndsTwoSessionsBack(t *testing.T) {
	withState(t)
	st := newFakeStore(BackfillEnd)
	if _, err := Run(context.Background(), deps(t, st, "dev"), StateQuiet); err != nil {
		t.Fatal(err)
	}
	newest := time.Time{}
	for _, ev := range st.events {
		if ev.SessionDate.After(newest) {
			newest = ev.SessionDate
		}
	}
	if !newest.Equal(sessionsBack(st, 2)) {
		t.Errorf("newest event on %s, want %s, two sessions back", newest.Format(time.DateOnly), sessionsBack(st, 2).Format(time.DateOnly))
	}
	if _, ok := st.events[key("MUFG", sessionsBack(st, 0))]; ok {
		t.Error("quiet recorded the newest session's trip")
	}
}

func TestASecondRunWritesNothingNew(t *testing.T) {
	withState(t)
	st := newFakeStore(BackfillEnd)
	if _, err := Run(context.Background(), deps(t, st, "dev"), StateTripped); err != nil {
		t.Fatal(err)
	}
	before := len(st.events)
	sums, err := Run(context.Background(), deps(t, st, "dev"), StateTripped)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range sums {
		if s.Inserted != 0 {
			t.Errorf("session %s inserted %d on the second run", s.Session.Format(time.DateOnly), s.Inserted)
		}
	}
	if len(st.events) != before {
		t.Errorf("%d events after the second run, want %d", len(st.events), before)
	}
}

func TestTheECBFeedNeverReachesTheNetwork(t *testing.T) {
	if _, err := (noNetwork{}).Daily(context.Background()); err == nil {
		t.Error("Daily answered")
	}
	if _, err := (noNetwork{}).History(context.Background()); err == nil {
		t.Error("History answered")
	}
}

func TestEveryPartNamesASensor(t *testing.T) {
	for _, p := range plans {
		for _, pk := range p {
			for _, sym := range pk.symbols {
				if !isSensor(sym) {
					t.Errorf("%s is picked but is not a sensor", sym)
				}
				if _, ok := parts[sym]; !ok && sym != Pending {
					t.Errorf("%s is picked but has no part in the story", sym)
				}
			}
		}
	}
}
