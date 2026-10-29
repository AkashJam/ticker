package coverage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/centralbank"
	"github.com/AkashJam/ticker/internal/dividend"
	"github.com/AkashJam/ticker/internal/edgar"
	"github.com/AkashJam/ticker/internal/news"
	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/trigger"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

var (
	session  = time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	closedAt = time.Date(2026, 10, 15, 20, 0, 0, 0, time.UTC)
)

func hr(day, hour int) time.Time { return time.Date(2026, 10, day, hour, 0, 0, 0, time.UTC) }

// --- fakes ---

type fakeConfig struct {
	sensors  map[string]SensorConfig
	excluded news.ExcludedList
}

func (f fakeConfig) Sensor(_ context.Context, s string) (SensorConfig, error) {
	c, ok := f.sensors[s]
	if !ok {
		return SensorConfig{}, errors.New("unknown sensor")
	}
	return c, nil
}
func (f fakeConfig) Excluded(context.Context) (news.ExcludedList, error) { return f.excluded, nil }

type fakeNews struct {
	byPhrase map[string][]news.Item
	errs     map[string]error
	calls    []string
	windows  [][2]time.Time
}

func (f *fakeNews) Search(_ context.Context, phrase string, after, before time.Time) ([]news.Item, error) {
	f.calls = append(f.calls, phrase)
	f.windows = append(f.windows, [2]time.Time{after, before})
	if err := f.errs[phrase]; err != nil {
		return nil, err
	}
	return f.byPhrase[phrase], nil
}

type fakeBank struct {
	items []centralbank.Item
	err   error
	calls int
}

func (f *fakeBank) Fetch(context.Context, reference.Feed) ([]centralbank.Item, error) {
	f.calls++
	return f.items, f.err
}

type fakeFilings struct {
	filings []edgar.Filing
	err     error
	calls   int
}

func (f *fakeFilings) Recent(context.Context, int) ([]edgar.Filing, error) {
	f.calls++
	return f.filings, f.err
}

type fakeDividends struct {
	div   *dividend.Dividend
	err   error
	calls []string
}

func (f *fakeDividends) ExDividend(_ context.Context, symbol string, _ time.Time) (*dividend.Dividend, error) {
	f.calls = append(f.calls, symbol)
	return f.div, f.err
}

type rig struct {
	c *Collector
	n *fakeNews
	b *fakeBank
	f *fakeFilings
	d *fakeDividends
}

const (
	econ   = "Mexico (economy)"
	sector = "America Movil"
	topic  = "semiconductor"
)

func newRig() *rig {
	r := &rig{n: &fakeNews{byPhrase: map[string][]news.Item{}, errs: map[string]error{}}, b: &fakeBank{}, f: &fakeFilings{}, d: &fakeDividends{}}
	cfg := fakeConfig{sensors: map[string]SensorConfig{
		"AMX": {Symbol: "AMX", EconomyQuery: econ, SectorQuery: sector, CIK: 1129137,
			FeedReason: "Banxico's RSS channels are all data series"},
		"RY": {Symbol: "RY", EconomyQuery: "Canada", SectorQuery: "RBC", CIK: 1000275,
			Feed: &reference.Feed{URL: "https://boc.example/feed", Format: "rss1", Language: "en"}},
		"ITUB": {Symbol: "ITUB", EconomyQuery: "Brazil", SectorQuery: "Itau", CIK: 1132597,
			Feed: &reference.Feed{URL: "https://bcb.example/feed", Format: "atom", Language: "pt"}},
		"NVDA": {Symbol: "NVDA", Global: true, Topic: topic, CIK: 1045810},
	}, excluded: news.ExcludedList{Excluded: []news.Entry{{Name: "MarketBeat", Hosts: []string{"marketbeat.com"}}}}}
	r.c = NewCollector(cfg, r.n, r.b, r.f, quiet())
	r.c.Dividends = r.d
	r.c.Sleep = func(context.Context, time.Duration) error { return nil }
	r.c.Now = func() time.Time { return time.Date(2026, 10, 15, 20, 20, 0, 0, time.UTC) }
	return r
}

func trip(sym string, label *string, splitPending bool) trigger.Trip {
	t := trigger.Trip{Symbol: sym, SessionDate: session, ClosedAt: closedAt, SplitLabel: label}
	if !splitPending {
		t.Split = []byte(`{"status":"ok"}`)
	}
	return t
}

func lbl(s string) *string { return &s }

func collect(t *testing.T, r *rig, ev trigger.Trip, prev []byte, stage string) *Coverage {
	t.Helper()
	b, err := r.c.Collect(context.Background(), ev, prev, stage)
	if err != nil {
		t.Fatal(err)
	}
	cov, err := Decode(b)
	if err != nil || cov == nil {
		t.Fatalf("decode: %v", err)
	}
	return cov
}

func names(m map[string]*Source) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// --- routing ---

func TestRouting(t *testing.T) {
	for _, tt := range []struct {
		name    string
		global  bool
		pending bool
		label   *string
		routing string
		want    []string
	}{
		{"currency-led asks the bank, the economy and the dividend", false, false, lbl(trigger.LabelCurrencyLed), trigger.LabelCurrencyLed, []string{SrcBank, SrcEconomy, SrcDividend}},
		{"stock-led asks the economy too, then the filings, the sector and the dividend", false, false, lbl(trigger.LabelStockLed), trigger.LabelStockLed, []string{SrcEconomy, SrcFilings, SrcSector, SrcDividend}},
		{"both asks everything", false, false, lbl(trigger.LabelBoth), trigger.LabelBoth, []string{SrcBank, SrcEconomy, SrcFilings, SrcSector, SrcDividend}},
		{"neither asks everything", false, false, lbl(trigger.LabelNeither), trigger.LabelNeither, []string{SrcBank, SrcEconomy, SrcFilings, SrcSector, SrcDividend}},
		{"a country with no split asks everything", false, false, nil, RoutingNone, []string{SrcBank, SrcEconomy, SrcFilings, SrcSector, SrcDividend}},
		{"a pending split asks everything", false, true, nil, RoutingPendingSplit, []string{SrcBank, SrcEconomy, SrcFilings, SrcSector, SrcDividend}},
		{"a global name asks its 8-Ks, its topic and the dividend", true, false, nil, RoutingGlobal, []string{SrcFilings, SrcTopic, SrcDividend}},
		{"a global name ignores a pending split", true, true, nil, RoutingGlobal, []string{SrcFilings, SrcTopic, SrcDividend}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			routing, got := Routing(tt.global, tt.pending, tt.label)
			if routing != tt.routing || strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Errorf("routing %q %v, want %q %v", routing, got, tt.routing, tt.want)
			}
		})
	}
}

// --- the trip's fetch ---

func TestTripFetchForABothTripWithNoBankFeed(t *testing.T) {
	r := newRig()
	r.n.byPhrase[econ] = []news.Item{
		{Title: "Mexico peso slides", Outlet: "Reuters", OutletURL: "https://www.reuters.com", Link: "r1", Published: hr(15, 14)},
		{Title: "Mexico peso slides again", Outlet: "Reuters", OutletURL: "https://www.reuters.com", Link: "r2", Published: hr(15, 21)},
		{Title: "Generated", Outlet: "MarketBeat", OutletURL: "https://www.marketbeat.com", Link: "m1", Published: hr(15, 14)},
	}
	r.n.byPhrase[sector] = nil
	r.f.filings = []edgar.Filing{
		{Form: "6-K", Accession: "0001-26-000001", Filed: session, Accepted: hr(15, 12), Description: "FORM 6-K", Document: "k.htm", URL: "https://sec/k"},
		{Form: "424B2", Filed: session, Accepted: hr(15, 13), Description: "FORM 424B2", URL: "https://sec/p"},
	}

	cov := collect(t, r, trip("AMX", lbl(trigger.LabelBoth), false), nil, trigger.StageTrip)
	if cov.Stage != trigger.StageTrip || cov.Complete || cov.Routing != trigger.LabelBoth {
		t.Errorf("header = %s %v %s", cov.Stage, cov.Complete, cov.Routing)
	}
	if cov.Window.From != "2026-10-14" || cov.Window.To != "2026-10-17" || !cov.ClosedAt.Equal(closedAt) {
		t.Errorf("window %+v, closedAt %s", cov.Window, cov.ClosedAt)
	}
	for _, w := range r.n.windows {
		if !w[0].Equal(hr(14, 0)) || !w[1].Equal(hr(17, 0)) {
			t.Errorf("search window = %v, want 14 Oct to 17 Oct (the day before, to two days after)", w)
		}
	}

	bank := cov.Sources[SrcBank]
	if bank.Status != StatusEmpty || bank.Absent == nil || !strings.Contains(bank.Absent.Reason, "Banxico") || r.b.calls != 0 {
		t.Errorf("bank = %+v, calls %d: a country with no feed is a stated absence, with no fetch", bank, r.b.calls)
	}
	e := cov.Sources[SrcEconomy]
	if e.Status != StatusOK || e.Attempts != 1 || e.Total != 3 || e.Excluded != 1 || len(e.Outlets) != 1 || e.Query != econ {
		t.Fatalf("economy = %+v", e)
	}
	if got := e.Outlets[0].Items; len(got) != 2 || got[0].Side != news.SideBefore || got[1].Side != news.SideAfter {
		t.Errorf("Reuters items = %+v, want one before and one after the close", got)
	}
	s := cov.Sources[SrcSector]
	if s.Status != StatusEmpty || s.Absent == nil || !strings.Contains(s.Absent.Reason, "no headlines") {
		t.Errorf("sector = %+v", s)
	}
	f := cov.Sources[SrcFilings]
	if f.Status != StatusOK || len(f.Items) != 1 || f.Items[0].Form != "6-K" || f.Items[0].Side != news.SideBefore || f.Categorised == nil || *f.Categorised {
		t.Errorf("filings = %+v: one 6-K (the 424B2 is not counted), uncategorised", f)
	}
}

func TestACurrencyLedTripAsksOnlyTheBankAndTheEconomy(t *testing.T) {
	r := newRig()
	r.b.items = []centralbank.Item{
		{Title: "Decision", Link: "b1", Published: hr(15, 15), Language: "en"},
		{Title: "Old release", Link: "b0", Published: hr(1, 15), Language: "en"},
		{Title: "Later release", Link: "b2", Published: hr(16, 15), Language: "en"},
	}
	cov := collect(t, r, trip("RY", lbl(trigger.LabelCurrencyLed), false), nil, trigger.StageTrip)
	if r.f.calls != 0 || len(r.n.calls) != 1 || r.n.calls[0] != "Canada" {
		t.Errorf("filings calls %d, news calls %v; a currency-led trip must not ask the filings or the sector", r.f.calls, r.n.calls)
	}
	if len(cov.Sources) != 3 || cov.Sources[SrcDividend] == nil {
		t.Errorf("sources = %v", names(cov.Sources))
	}
	b := cov.Sources[SrcBank]
	if b.Status != StatusOK || len(b.Items) != 2 || b.Items[0].Title != "Decision" || b.Items[0].Side != news.SideBefore || b.Items[1].Side != news.SideAfter {
		t.Errorf("bank = %+v: the in-window releases, oldest first, the old one left out", b)
	}
}

func TestBankKeepsItsLanguageAndCapsItsItems(t *testing.T) {
	r := newRig()
	for i := range 8 {
		r.b.items = append(r.b.items, centralbank.Item{Title: "p", Link: string(rune('a' + i)), Published: hr(15, 1+i), Language: "pt"})
	}
	cov := collect(t, r, trip("ITUB", lbl(trigger.LabelCurrencyLed), false), nil, trigger.StageTrip)
	b := cov.Sources[SrcBank]
	if len(b.Items) != 5 || b.Items[0].Language != "pt" {
		t.Errorf("bank items %d, first %+v", len(b.Items), b.Items[0])
	}
	// An empty window is a stated absence.
	r2 := newRig()
	r2.b.items = []centralbank.Item{{Title: "old", Link: "x", Published: hr(1, 1)}}
	b2 := collect(t, r2, trip("ITUB", lbl(trigger.LabelCurrencyLed), false), nil, trigger.StageTrip).Sources[SrcBank]
	if b2.Status != StatusEmpty || b2.Absent == nil || !strings.Contains(b2.Absent.Reason, "no release") {
		t.Errorf("bank = %+v", b2)
	}
}

func TestAGlobalNameGetsItsEightKsWithItemCodesAndItsTopic(t *testing.T) {
	r := newRig()
	r.n.byPhrase[topic] = []news.Item{{Title: "Semiconductor stocks rally", Outlet: "WSJ", OutletURL: "https://www.wsj.com", Link: "w1", Published: hr(15, 15)}}
	r.f.filings = []edgar.Filing{
		{Form: "8-K", Filed: session, Accepted: hr(15, 21), Items: []string{"2.02", "9.01"}, Description: "8-K", URL: "https://sec/e"},
		{Form: "8-K/A", Filed: hr(14, 0), Accepted: hr(14, 21), Items: []string{"5.02"}, Description: "8-K/A", URL: "https://sec/a"},
	}
	cov := collect(t, r, trip("NVDA", nil, false), nil, trigger.StageTrip)
	if cov.Routing != RoutingGlobal || len(cov.Sources) != 3 {
		t.Fatalf("routing %s, sources %v", cov.Routing, names(cov.Sources))
	}
	f := cov.Sources[SrcFilings]
	if len(f.Items) != 2 || f.Categorised == nil || !*f.Categorised {
		t.Fatalf("filings = %+v", f)
	}
	if f.Items[0].Form != "8-K/A" || f.Items[1].Side != news.SideAfter {
		t.Errorf("items = %+v: oldest first, the 15 Oct 21:00 filing after the close", f.Items)
	}
	codes := f.Items[1].ItemCodes
	if len(codes) != 2 || codes[0].Code != "2.02" || codes[0].Title != "Results of Operations and Financial Condition" {
		t.Errorf("item codes = %+v", codes)
	}
	if cov.Sources[SrcTopic].Status != StatusOK {
		t.Errorf("topic = %+v", cov.Sources[SrcTopic])
	}
}

func TestNoFilingIsAStatedAbsence(t *testing.T) {
	r := newRig()
	cov := collect(t, r, trip("AMX", lbl(trigger.LabelStockLed), false), nil, trigger.StageTrip)
	f := cov.Sources[SrcFilings]
	if f.Status != StatusEmpty || f.Absent == nil || f.Absent.Reason != "no 6-K was filed in the window" {
		t.Errorf("filings = %+v", f)
	}
	r2 := newRig()
	g := collect(t, r2, trip("NVDA", nil, false), nil, trigger.StageTrip).Sources[SrcFilings]
	if g.Absent == nil || g.Absent.Reason != "no 8-K was filed in the window" {
		t.Errorf("global filings = %+v", g)
	}
}

// --- failures ---

func TestAFailingSourceDoesNotBlockTheOthers(t *testing.T) {
	r := newRig()
	r.n.errs[econ] = &news.StatusError{Code: 429}
	r.n.byPhrase[sector] = []news.Item{{Title: "America Movil shares fall", Outlet: "AP", OutletURL: "https://apnews.com", Link: "a", Published: hr(15, 10)}}
	cov := collect(t, r, trip("AMX", lbl(trigger.LabelBoth), false), nil, trigger.StageTrip)
	e := cov.Sources[SrcEconomy]
	if e.Status != StatusError || e.Attempts != 1 || !strings.Contains(e.Error, "429") {
		t.Errorf("economy = %+v", e)
	}
	if cov.Sources[SrcSector].Status != StatusOK || cov.Sources[SrcFilings].Status != StatusEmpty {
		t.Error("the other sources must be written")
	}
	if cov.Complete {
		t.Error("a trip-stage coverage is never complete")
	}
}

func TestAnErrorIsRetriedThenFrozenAfterThreeAttempts(t *testing.T) {
	r := newRig()
	r.n.errs[econ] = errors.New("boom")
	ev := trip("AMX", lbl(trigger.LabelCurrencyLed), false)
	r.b.items = []centralbank.Item{{Title: "x", Link: "b", Published: hr(15, 10)}}

	cov := collect(t, r, ev, nil, trigger.StageTrip) // attempt 1
	prev, _ := marshal(cov)
	cov = collect(t, r, ev, prev, trigger.StageFollowUp) // attempt 2
	if cov.Sources[SrcEconomy].Attempts != 2 || cov.Complete {
		t.Fatalf("after the follow-up: %+v complete %v; one attempt left", cov.Sources[SrcEconomy], cov.Complete)
	}
	prev, _ = marshal(cov)
	cov = collect(t, r, ev, prev, trigger.StageFollowUp) // attempt 3
	e := cov.Sources[SrcEconomy]
	if e.Attempts != 3 || e.Status != StatusError || !cov.Complete {
		t.Fatalf("after three attempts: %+v complete %v; frozen as an error, and the coverage is as complete as it will get", e, cov.Complete)
	}
	calls := len(r.n.calls)
	prev, _ = marshal(cov)
	collect(t, r, ev, prev, trigger.StageFollowUp)
	if len(r.n.calls) != calls {
		t.Error("a frozen source must not be fetched again")
	}
}

func TestAnErrorThatRecoversKeepsTheRest(t *testing.T) {
	r := newRig()
	r.n.errs[econ] = errors.New("boom")
	ev := trip("AMX", lbl(trigger.LabelCurrencyLed), false)
	r.b.items = []centralbank.Item{{Title: "x", Link: "b", Published: hr(15, 10)}}
	prev, _ := marshal(collect(t, r, ev, nil, trigger.StageTrip))

	delete(r.n.errs, econ)
	r.n.byPhrase[econ] = []news.Item{{Title: "Mexico peso", Outlet: "Reuters", OutletURL: "https://www.reuters.com", Link: "r", Published: hr(15, 10)}}
	cov := collect(t, r, ev, prev, trigger.StageFollowUp)
	if e := cov.Sources[SrcEconomy]; e.Status != StatusOK || e.Attempts != 2 {
		t.Errorf("economy = %+v", e)
	}
	if !cov.Complete {
		t.Error("with no source in error the follow-up is complete")
	}
}

func TestWithoutAContactEDGARIsAnErrorFrozenAtOnce(t *testing.T) {
	r := newRig()
	r.c.Filings = nil
	r.n.byPhrase[sector] = []news.Item{{Title: "America Movil shares fall", Outlet: "AP", OutletURL: "https://apnews.com", Link: "a", Published: hr(15, 10)}}
	cov := collect(t, r, trip("AMX", lbl(trigger.LabelStockLed), false), nil, trigger.StageTrip)
	f := cov.Sources[SrcFilings]
	if f.Status != StatusError || f.Attempts != MaxAttempts || !strings.Contains(f.Error, "FETCH_CONTACT") {
		t.Errorf("filings = %+v", f)
	}
	if cov.Sources[SrcSector].Status != StatusOK {
		t.Error("the news must still be fetched")
	}
	prev, _ := marshal(cov)
	cov = collect(t, r, trip("AMX", lbl(trigger.LabelStockLed), false), prev, trigger.StageFollowUp)
	if !cov.Complete {
		t.Error("a frozen error does not keep the coverage open")
	}
}

func TestAMissingConfigurationIsFrozenToo(t *testing.T) {
	r := newRig()
	r.c.Config.(fakeConfig).sensors["AMX"] = SensorConfig{Symbol: "AMX", CIK: 0}
	cov := collect(t, r, trip("AMX", lbl(trigger.LabelBoth), false), nil, trigger.StageTrip)
	for _, src := range []string{SrcEconomy, SrcSector, SrcFilings} {
		if s := cov.Sources[src]; s.Status != StatusError || s.Attempts != MaxAttempts {
			t.Errorf("%s = %+v, want a frozen error", src, s)
		}
	}
}

// --- the follow-up ---

func TestFollowUpAddsTheAfterTheCloseSideAndKeepsWhatWasFound(t *testing.T) {
	r := newRig()
	ev := trip("AMX", lbl(trigger.LabelStockLed), false)
	r.n.byPhrase[sector] = []news.Item{{Title: "America Movil before the close", Outlet: "Reuters", OutletURL: "https://www.reuters.com", Link: "r1", Published: hr(15, 14)}}
	r.f.filings = []edgar.Filing{{Form: "6-K", Filed: session, Accepted: hr(15, 12), Description: "FORM 6-K", URL: "https://sec/k1"}}
	first := collect(t, r, ev, nil, trigger.StageTrip)
	prev, _ := marshal(first)

	// Next day: Google's page no longer holds the earlier item, and there is a reaction.
	r.n.byPhrase[sector] = []news.Item{{Title: "America Movil reaction", Outlet: "AP", OutletURL: "https://apnews.com", Link: "a1", Published: hr(16, 13)}}
	r.f.filings = append(r.f.filings, edgar.Filing{Form: "6-K", Filed: hr(16, 0), Accepted: hr(16, 12), Description: "FORM 6-K", URL: "https://sec/k2"})
	cov := collect(t, r, ev, prev, trigger.StageFollowUp)

	if cov.Stage != trigger.StageFollowUp || !cov.Complete {
		t.Errorf("stage %s complete %v", cov.Stage, cov.Complete)
	}
	s := cov.Sources[SrcSector]
	if len(s.Outlets) != 2 || s.Outlets[0].Name != "Reuters" || s.Outlets[1].Items[0].Side != news.SideAfter || s.Attempts != 2 {
		t.Errorf("sector = %+v: the earlier Reuters item must survive and the AP reaction be added", s)
	}
	f := cov.Sources[SrcFilings]
	if len(f.Items) != 2 || f.Items[0].Side != news.SideBefore || f.Items[1].Side != news.SideAfter {
		t.Errorf("filings = %+v", f.Items)
	}
}

func TestFollowUpFetchesOnceAndASecondFollowUpFetchesNothing(t *testing.T) {
	r := newRig()
	ev := trip("AMX", lbl(trigger.LabelStockLed), false)
	prev, _ := marshal(collect(t, r, ev, nil, trigger.StageTrip))
	prev, _ = marshal(collect(t, r, ev, prev, trigger.StageFollowUp))
	calls, fcalls := len(r.n.calls), r.f.calls
	collect(t, r, ev, prev, trigger.StageFollowUp)
	if len(r.n.calls) != calls || r.f.calls != fcalls {
		t.Error("once complete, a coverage is not fetched again")
	}
}

func TestNewsSearchesArePaced(t *testing.T) {
	r := newRig()
	var slept []time.Duration
	r.c.Sleep = func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil }
	// Now never advances, so the second search must wait the gap.
	collect(t, r, trip("AMX", lbl(trigger.LabelBoth), false), nil, trigger.StageTrip)
	if len(slept) != 1 || slept[0] != r.c.NewsGap {
		t.Errorf("slept %v, want one wait of the gap between the two news searches", slept)
	}
}

func TestACanceledContextStopsTheCollection(t *testing.T) {
	r := newRig()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.n.errs[econ] = context.Canceled
	_, err := r.c.Collect(ctx, trip("AMX", lbl(trigger.LabelBoth), false), nil, trigger.StageTrip)
	if err == nil {
		t.Error("a canceled context must surface, not be recorded as a source error")
	}
}

func TestMergeNewsNeverDropsAnEarlierHeadline(t *testing.T) {
	prev := &Source{Outlets: []news.Outlet{{Name: "Reuters", URL: "https://r", Items: []news.GroupedItem{
		{Title: "kept only by the first fetch", Link: "l3", Published: hr(15, 1)},
		{Title: "also in the new page", Link: "l1", Published: hr(15, 2)},
	}}}}
	got := mergeNews(prev, []news.Item{{Title: "new", Link: "l2", Outlet: "Reuters", Published: hr(16, 1)}, {Title: "also in the new page", Link: "l1", Outlet: "Reuters", Published: hr(15, 2)}})
	links := map[string]int{}
	for _, it := range got {
		links[it.Link]++
	}
	if len(got) != 3 || links["l1"] != 1 || links["l2"] != 1 || links["l3"] != 1 {
		t.Errorf("merged = %v, want l1, l2 and l3 once each", links)
	}
	if mergeNews(nil, nil) != nil {
		t.Error("nothing merged with nothing is nothing")
	}
}

func marshal(c *Coverage) ([]byte, error)     { return jsonMarshal(c) }
func jsonMarshal(c *Coverage) ([]byte, error) { return json.Marshal(c) }

func TestAPendingSplitAsksEverythingAndSaysSo(t *testing.T) {
	r := newRig()
	cov := collect(t, r, trip("AMX", nil, true), nil, trigger.StageTrip)
	if cov.Routing != RoutingPendingSplit || len(cov.Sources) != 5 {
		t.Errorf("routing %s, sources %v", cov.Routing, names(cov.Sources))
	}
}

// --- the dividend source (bearing.md #112) ---

func TestAnExDividendSessionIsNamedWithItsDateAndAmount(t *testing.T) {
	r := newRig()
	r.d.div = &dividend.Dividend{Date: "2026-10-15", Amount: 1.249, Currency: "USD"}
	cov := collect(t, r, trip("RY", lbl(trigger.LabelStockLed), false), nil, trigger.StageTrip)
	d := cov.Sources[SrcDividend]
	if d == nil || d.Status != StatusOK || d.Dividend == nil || d.Dividend.Amount != 1.249 || d.Dividend.Date != "2026-10-15" || d.Dividend.Currency != "USD" {
		t.Fatalf("dividend = %+v", d)
	}
	if d.Attempts != 1 || d.Absent != nil || d.Query == "" {
		t.Errorf("dividend = %+v: one attempt, no absence, the source named", d)
	}
	if len(r.d.calls) != 1 || r.d.calls[0] != "RY" {
		t.Errorf("calls = %v: one lookup for the tripped sensor", r.d.calls)
	}
}

func TestNoExDividendIsAStatedAbsence(t *testing.T) {
	r := newRig()
	d := collect(t, r, trip("AMX", lbl(trigger.LabelBoth), false), nil, trigger.StageTrip).Sources[SrcDividend]
	if d.Status != StatusEmpty || d.Dividend != nil || d.Absent == nil || d.Absent.Reason != "no ex-dividend date on this session" {
		t.Errorf("dividend = %+v", d)
	}
}

func TestEveryRouteAsksTheDividendOnce(t *testing.T) {
	for _, ev := range []trigger.Trip{
		trip("RY", lbl(trigger.LabelCurrencyLed), false),
		trip("AMX", lbl(trigger.LabelStockLed), false),
		trip("AMX", lbl(trigger.LabelNeither), false),
		trip("AMX", nil, true),
		trip("NVDA", nil, false),
	} {
		r := newRig()
		cov := collect(t, r, ev, nil, trigger.StageTrip)
		if cov.Sources[SrcDividend] == nil || len(r.d.calls) != 1 {
			t.Errorf("%s (%s): dividend %+v, calls %v", ev.Symbol, cov.Routing, cov.Sources[SrcDividend], r.d.calls)
		}
	}
}

func TestAFailedDividendLookupIsRetriedThenFrozenAfterThreeAttempts(t *testing.T) {
	r := newRig()
	r.d.err = errors.New("dividend: RY: yahoo returned HTTP 429")
	ev := trip("RY", lbl(trigger.LabelStockLed), false)

	cov := collect(t, r, ev, nil, trigger.StageTrip)
	d := cov.Sources[SrcDividend]
	if d.Status != StatusError || d.Attempts != 1 || !strings.Contains(d.Error, "429") {
		t.Fatalf("trip: %+v", d)
	}
	b, _ := json.Marshal(cov)
	cov = collect(t, r, ev, b, trigger.StageFollowUp)
	if d = cov.Sources[SrcDividend]; d.Attempts != 2 || d.Status != StatusError {
		t.Fatalf("follow-up: %+v", d)
	}
	b, _ = json.Marshal(cov)
	cov = collect(t, r, ev, b, trigger.StageFollowUp)
	if d = cov.Sources[SrcDividend]; d.Attempts != 3 || d.Status != StatusError {
		t.Fatalf("third: %+v", d)
	}
	b, _ = json.Marshal(cov)
	before := len(r.d.calls)
	cov = collect(t, r, ev, b, trigger.StageFollowUp)
	if len(r.d.calls) != before || cov.Sources[SrcDividend].Attempts != 3 {
		t.Errorf("a source at three attempts is frozen: calls %d to %d", before, len(r.d.calls))
	}
}

func TestADividendThatAnsweredEmptyAtTheTripIsAskedAgainAtTheFollowUp(t *testing.T) {
	// Yahoo may not list the same day's ex-date at 16:15 ET; the follow-up asks
	// every source again, so the next session's answer corrects it.
	r := newRig()
	ev := trip("RY", lbl(trigger.LabelStockLed), false)
	cov := collect(t, r, ev, nil, trigger.StageTrip)
	if cov.Sources[SrcDividend].Status != StatusEmpty {
		t.Fatalf("trip: %+v", cov.Sources[SrcDividend])
	}
	b, _ := json.Marshal(cov)
	r.d.div = &dividend.Dividend{Date: "2026-10-15", Amount: 0.8, Currency: "USD"}
	cov = collect(t, r, ev, b, trigger.StageFollowUp)
	if d := cov.Sources[SrcDividend]; d.Status != StatusOK || d.Attempts != 2 || d.Dividend == nil {
		t.Errorf("follow-up: %+v", d)
	}
}

func TestWithoutADividendSourceTheSourceIsAnErrorFrozenAtOnce(t *testing.T) {
	r := newRig()
	r.c.Dividends = nil
	d := collect(t, r, trip("RY", lbl(trigger.LabelStockLed), false), nil, trigger.StageTrip).Sources[SrcDividend]
	if d.Status != StatusError || d.Attempts != MaxAttempts || !strings.Contains(d.Error, "not configured") {
		t.Errorf("dividend = %+v", d)
	}
}

func TestADividendFailureDoesNotBlockTheNews(t *testing.T) {
	r := newRig()
	r.d.err = errors.New("blocked")
	r.n.byPhrase[sector] = []news.Item{{Title: "America Movil shares fall", Outlet: "AP", OutletURL: "https://apnews.com", Link: "a", Published: hr(15, 10)}}
	cov := collect(t, r, trip("AMX", lbl(trigger.LabelBoth), false), nil, trigger.StageTrip)
	if cov.Sources[SrcDividend].Status != StatusError || cov.Sources[SrcSector].Status != StatusOK {
		t.Errorf("dividend %+v, sector %+v", cov.Sources[SrcDividend], cov.Sources[SrcSector])
	}
}
