// Package coverage fetches the explanation for a recorded trip (portfolio.md §15
// Phase 15 step 6): the central bank's releases, EDGAR filings, and Google News,
// routed by the trip's currency split, on a trip and never on a timer. It stores
// every outlet without ranking and states every absence, so a quiet section is
// never mistaken for a missing one.
package coverage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/AkashJam/ticker/internal/centralbank"
	"github.com/AkashJam/ticker/internal/dividend"
	"github.com/AkashJam/ticker/internal/edgar"
	"github.com/AkashJam/ticker/internal/news"
	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/trigger"
)

// The sources an event can ask.
const (
	SrcBank    = "centralBank"
	SrcEconomy = "economyNews"
	SrcSector  = "sectorNews"
	SrcTopic   = "topicNews"
	SrcFilings = "filings"
	// SrcDividend asks whether the session was an ex-dividend date (bearing.md
	// #112). It is a caveat rather than an explanation, so the page reads it as a
	// line under the trip and never lists it with the news.
	SrcDividend = "dividend"
)

// Statuses of one source.
const (
	StatusOK    = "ok"
	StatusEmpty = "empty" // answered, and there was nothing: a stated absence
	StatusError = "error"
)

// MaxAttempts is how many times a source is tried in all (decision 7): the
// trip, the follow-up, and the run after.
const MaxAttempts = 3

// Routings other than a split label.
const (
	RoutingNone         = "none"          // a country sensor with no split: ask everything
	RoutingPendingSplit = "pending-split" // the split is not final yet: ask everything
	RoutingGlobal       = "global"        // a global name: its own 8-Ks and its topic
)

var coverageTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ticker_trigger_coverage_total",
	Help: "Source fetches for a trip's coverage, by source and result (ok, empty, error).",
}, []string{"source", "result"})

// Absent says why a source holds nothing.
type Absent struct {
	Reason string `json:"reason"`
}

// Item is one entry of a bank or filings source.
type Item struct {
	Title       string     `json:"title"`
	Link        string     `json:"link"`
	PublishedAt time.Time  `json:"publishedAt"`
	Side        string     `json:"side"`
	Language    string     `json:"language,omitempty"`
	Form        string     `json:"form,omitempty"`
	ItemCodes   []ItemCode `json:"itemCodes,omitempty"`
	_           struct{}   `json:"-"`
}

// ItemCode is an 8-K item with the SEC's title.
type ItemCode struct {
	Code  string `json:"code"`
	Title string `json:"title,omitempty"`
}

// Source is one source's result.
type Source struct {
	Status    string  `json:"status"`
	Attempts  int     `json:"attempts"`
	FetchedAt string  `json:"fetchedAt"`
	Error     string  `json:"error,omitempty"`
	Absent    *Absent `json:"absent,omitempty"`
	// Query is the phrase or feed that was asked, for provenance.
	Query string `json:"query,omitempty"`

	// News sources.
	Outlets    []news.Outlet `json:"outlets,omitempty"`
	Total      int           `json:"total,omitempty"`
	Excluded   int           `json:"excluded,omitempty"`
	NonEnglish int           `json:"nonEnglish,omitempty"`
	Undated    int           `json:"undated,omitempty"`
	NoSubject  int           `json:"noSubject,omitempty"`
	Duplicates int           `json:"duplicates,omitempty"`

	// Bank and filings sources.
	Items []Item `json:"items,omitempty"`
	// Categorised is false for 6-Ks, which carry no item codes: foreign filers
	// are detected but not categorised, and the page says so.
	Categorised *bool `json:"categorised,omitempty"`

	// Dividend source: set when the session was an ex-dividend date.
	Dividend *dividend.Dividend `json:"dividend,omitempty"`
}

// Window is the dates a coverage looked at: from inclusive, to exclusive.
type Window struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Coverage is the JSON stored in events.coverage.
type Coverage struct {
	Stage    string             `json:"stage"`
	Complete bool               `json:"complete"`
	Routing  string             `json:"routing"`
	Window   Window             `json:"window"`
	ClosedAt time.Time          `json:"closedAt"`
	Sources  map[string]*Source `json:"sources"`
}

// SensorConfig is what the collector needs to know about a sensor.
type SensorConfig struct {
	Symbol       string
	Global       bool
	EconomyQuery string
	SectorQuery  string
	Topic        string
	Feed         *reference.Feed
	FeedReason   string
	CIK          int // 0 when EDGAR has no identity for it yet
}

// Config supplies per-sensor configuration and the excluded-outlets list.
type Config interface {
	Sensor(ctx context.Context, symbol string) (SensorConfig, error)
	Excluded(ctx context.Context) (news.ExcludedList, error)
}

// The three outside sources, as interfaces so tests can fake them.
type (
	NewsSearcher interface {
		Search(ctx context.Context, phrase string, after, before time.Time) ([]news.Item, error)
	}
	BankFetcher interface {
		Fetch(ctx context.Context, feed reference.Feed) ([]centralbank.Item, error)
	}
	FilingSource interface {
		Recent(ctx context.Context, cik int) ([]edgar.Filing, error)
	}
	// DividendSource reports the dividend that went ex on a session, or nil.
	DividendSource interface {
		ExDividend(ctx context.Context, symbol string, session time.Time) (*dividend.Dividend, error)
	}
)

// Collector fetches coverage. FilingSource may be nil: EDGAR needs a contact,
// and without one the filings source is an error frozen at once. Dividends may be
// nil in the same way.
type Collector struct {
	Config    Config
	News      NewsSearcher
	Bank      BankFetcher
	Filings   FilingSource
	Dividends DividendSource
	Now       func() time.Time
	Sleep     func(ctx context.Context, d time.Duration) error
	NewsGap   time.Duration
	Log       *slog.Logger
	lastNews  time.Time
}

// NewCollector returns a collector with the production pacing: a second between
// Google News calls.
func NewCollector(cfg Config, n NewsSearcher, b BankFetcher, f FilingSource, log *slog.Logger) *Collector {
	return &Collector{Config: cfg, News: n, Bank: b, Filings: f, Now: time.Now, Sleep: sleepCtx, NewsGap: time.Second, Log: log}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Decode reads stored coverage. Nil or empty bytes give nil.
func Decode(b []byte) (*Coverage, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var c Coverage
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("coverage: decode: %w", err)
	}
	if c.Sources == nil {
		c.Sources = map[string]*Source{}
	}
	return &c, nil
}

// Routing names how an event is routed and which sources it asks.
func Routing(global bool, splitPending bool, label *string) (string, []string) {
	// Every route also asks the dividend source: whether the session was an
	// ex-date is a fact about the move, whichever part of it the split blames.
	if global {
		return RoutingGlobal, []string{SrcFilings, SrcTopic, SrcDividend}
	}
	all := []string{SrcBank, SrcEconomy, SrcFilings, SrcSector, SrcDividend}
	// The economy is asked on every country trip (M17): a stock-led split says the
	// currency was quiet, not that the whole market was.
	switch {
	case splitPending:
		return RoutingPendingSplit, all
	case label == nil:
		return RoutingNone, all
	}
	switch *label {
	case trigger.LabelCurrencyLed:
		return *label, []string{SrcBank, SrcEconomy, SrcDividend}
	case trigger.LabelStockLed:
		return *label, []string{SrcEconomy, SrcFilings, SrcSector, SrcDividend}
	}
	return *label, all // both, neither
}

// Window returns the dates fetched for a session: from the day before it
// (inclusive) to two days after (exclusive).
func WindowFor(session time.Time) (from, to time.Time) {
	d := session.UTC().Truncate(24 * time.Hour)
	return d.AddDate(0, 0, -1), d.AddDate(0, 0, 2)
}

// Collect fetches the sources an event asks and merges them into prev (nil on
// the first fetch), returning the coverage JSON to store. stage is trigger.StageTrip for
// the first fetch and trigger.StageFollowUp afterwards. A source that fails is recorded
// and does not stop the others; ctx ending does.
func (c *Collector) Collect(ctx context.Context, ev trigger.Trip, prev []byte, stage string) ([]byte, error) {
	cfg, err := c.Config.Sensor(ctx, ev.Symbol)
	if err != nil {
		return nil, fmt.Errorf("coverage: %s: %w", ev.Symbol, err)
	}
	excluded, err := c.Config.Excluded(ctx)
	if err != nil {
		return nil, fmt.Errorf("coverage: excluded outlets: %w", err)
	}
	old, err := Decode(prev)
	if err != nil {
		return nil, err
	}

	routing, wanted := Routing(cfg.Global, ev.SplitPending(), ev.SplitLabel)
	from, to := WindowFor(ev.SessionDate)
	cov := &Coverage{
		Stage: stage, Routing: routing, ClosedAt: ev.ClosedAt.UTC(),
		Window:  Window{From: from.Format(time.DateOnly), To: to.Format(time.DateOnly)},
		Sources: map[string]*Source{},
	}
	if old != nil {
		for k, v := range old.Sources {
			cov.Sources[k] = v
		}
	}
	firstFollowUp := old != nil && old.Stage == trigger.StageTrip && stage == trigger.StageFollowUp

	for _, name := range wanted {
		prevSrc := cov.Sources[name]
		retry := prevSrc != nil && prevSrc.Status == StatusError && prevSrc.Attempts < MaxAttempts
		if prevSrc != nil && !firstFollowUp && !retry {
			continue
		}
		next, err := c.fetch(ctx, name, ev, cfg, from, to, excluded, prevSrc)
		if err != nil {
			return nil, err // the context ended
		}
		cov.Sources[name] = next
		coverageTotal.WithLabelValues(name, next.Status).Inc()
	}

	cov.Complete = stage == trigger.StageFollowUp
	for _, s := range cov.Sources {
		if s.Status == StatusError && s.Attempts < MaxAttempts {
			cov.Complete = false
		}
	}
	return json.Marshal(cov)
}

func (c *Collector) fetch(ctx context.Context, name string, ev trigger.Trip, cfg SensorConfig, from, to time.Time, excluded news.ExcludedList, prev *Source) (*Source, error) {
	attempts := 1
	if prev != nil {
		attempts = prev.Attempts + 1
	}
	var s *Source
	var err error
	switch name {
	case SrcBank:
		s, err = c.bank(ctx, cfg, from, to, ev.ClosedAt, prev)
	case SrcEconomy:
		s, err = c.newsSource(ctx, cfg.EconomyQuery, from, to, ev.ClosedAt, excluded, prev)
	case SrcSector:
		s, err = c.newsSource(ctx, cfg.SectorQuery, from, to, ev.ClosedAt, excluded, prev)
	case SrcTopic:
		s, err = c.newsSource(ctx, cfg.Topic, from, to, ev.ClosedAt, excluded, prev)
	case SrcFilings:
		s, err = c.filings(ctx, cfg, from, to, ev.ClosedAt, prev)
	case SrcDividend:
		s, err = c.dividend(ctx, ev)
	default:
		return nil, fmt.Errorf("coverage: unknown source %q", name)
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		c.Log.Warn("coverage: source failed", "symbol", ev.Symbol, "source", name, "attempt", attempts, "error", err)
		s = failed(prev, err)
	}
	s.Attempts = attempts
	if s.Status == StatusError && permanent(err) {
		s.Attempts = MaxAttempts // frozen at once: trying again cannot help
	}
	s.FetchedAt = c.Now().UTC().Format(time.RFC3339)
	return s, nil
}

// failed records an error and keeps what an earlier attempt found.
func failed(prev *Source, err error) *Source {
	s := &Source{Status: StatusError, Error: err.Error()}
	if prev != nil && prev.Status != StatusError {
		cp := *prev
		cp.Status, cp.Error = StatusError, err.Error()
		return &cp
	}
	return s
}

// errNotConfigured is an error retrying cannot fix.
var errNotConfigured = errors.New("not configured")

func permanent(err error) bool { return errors.Is(err, errNotConfigured) }

func (c *Collector) newsSource(ctx context.Context, phrase string, from, to, closedAt time.Time, excluded news.ExcludedList, prev *Source) (*Source, error) {
	if strings.TrimSpace(phrase) == "" {
		return nil, fmt.Errorf("no query phrase in the reference table: %w", errNotConfigured)
	}
	if wait := c.NewsGap - c.Now().Sub(c.lastNews); wait > 0 && !c.lastNews.IsZero() {
		if err := c.Sleep(ctx, wait); err != nil {
			return nil, err
		}
	}
	c.lastNews = c.Now()
	items, err := c.News.Search(ctx, phrase, from, to)
	if err != nil {
		return nil, err
	}
	// A follow-up adds to what the trip found: Google returns a bounded page, so
	// an early item can drop off it. Union by link, then group again.
	items = mergeNews(prev, items)
	res := news.Group(items, closedAt, excluded, phrase)
	s := &Source{Status: StatusOK, Query: phrase, Outlets: res.Outlets, Total: res.Total, Excluded: res.Excluded, NonEnglish: res.NonEnglish, Undated: res.Undated, NoSubject: res.NoSubject, Duplicates: res.Duplicates}
	if len(res.Outlets) == 0 {
		s.Status = StatusEmpty
		s.Absent = &Absent{Reason: newsAbsence(res)}
	}
	return s, nil
}

func newsAbsence(r news.Result) string {
	switch {
	case r.Total == 0:
		return "the query returned no headlines in the window"
	case r.Excluded == r.Total:
		return fmt.Sprintf("all %d headlines came from outlets that generate pages from price or filing data", r.Total)
	}
	return fmt.Sprintf("none of the %d headlines could be used (%d from machine-generated outlets, %d not in English, %d undated, %d naming none of the query's terms, %d repeated)", r.Total, r.Excluded, r.NonEnglish, r.Undated, r.NoSubject, r.Duplicates)
}

func mergeNews(prev *Source, fresh []news.Item) []news.Item {
	if prev == nil || len(prev.Outlets) == 0 {
		return fresh
	}
	seen := map[string]bool{}
	for _, it := range fresh {
		seen[it.Link] = true
	}
	out := append([]news.Item(nil), fresh...)
	for _, o := range prev.Outlets {
		for _, it := range o.Items {
			if seen[it.Link] {
				continue
			}
			out = append(out, news.Item{Title: it.Title, Link: it.Link, Outlet: o.Name, OutletURL: o.URL, Published: it.Published})
		}
	}
	return out
}

func (c *Collector) bank(ctx context.Context, cfg SensorConfig, from, to, closedAt time.Time, prev *Source) (*Source, error) {
	if cfg.Feed == nil {
		reason := cfg.FeedReason
		if reason == "" {
			reason = "no central bank feed is known for this country"
		}
		return &Source{Status: StatusEmpty, Absent: &Absent{Reason: reason}}, nil
	}
	items, err := c.Bank.Fetch(ctx, *cfg.Feed)
	if err != nil {
		return nil, err
	}
	got := centralbank.InWindow(items, from, to)
	s := &Source{Status: StatusOK, Query: cfg.Feed.URL}
	seen := map[string]bool{}
	add := func(it Item) {
		if seen[it.Link] || len(s.Items) >= maxBank {
			return
		}
		seen[it.Link] = true
		s.Items = append(s.Items, it)
	}
	for _, it := range got {
		add(Item{Title: it.Title, Link: it.Link, PublishedAt: it.Published, Side: side(it.Published, closedAt), Language: it.Language})
	}
	if prev != nil {
		for _, it := range prev.Items {
			add(it)
		}
	}
	sortItems(s.Items)
	if len(s.Items) == 0 {
		s.Status = StatusEmpty
		s.Absent = &Absent{Reason: "the bank published no release in the window"}
	}
	return s, nil
}

// maxBank caps a bank's items: a busy bank can release several a day.
const maxBank = 5

func (c *Collector) filings(ctx context.Context, cfg SensorConfig, from, to, closedAt time.Time, prev *Source) (*Source, error) {
	if c.Filings == nil {
		return nil, fmt.Errorf("EDGAR is not configured (FETCH_CONTACT is not set): %w", errNotConfigured)
	}
	if cfg.CIK == 0 {
		return nil, fmt.Errorf("no EDGAR identity for %s in the edgar dataset: %w", cfg.Symbol, errNotConfigured)
	}
	form := "6-K"
	if cfg.Global {
		form = "8-K"
	}
	all, err := c.Filings.Recent(ctx, cfg.CIK)
	if err != nil {
		return nil, err
	}
	// Filed from the day before the session through the day after, inclusive.
	picked := edgar.Select(all, []string{form}, from, to.AddDate(0, 0, -1))
	s := &Source{Status: StatusOK, Query: fmt.Sprintf("CIK %010d, form %s", cfg.CIK, form)}
	cat := cfg.Global
	s.Categorised = &cat
	seen := map[string]bool{}
	add := func(it Item) {
		if !seen[it.Link] {
			seen[it.Link] = true
			s.Items = append(s.Items, it)
		}
	}
	for _, f := range picked {
		it := Item{Title: f.Description, Link: f.URL, PublishedAt: f.Accepted, Side: side(f.Accepted, closedAt), Form: f.Form}
		for _, code := range f.Items {
			it.ItemCodes = append(it.ItemCodes, ItemCode{Code: code, Title: edgar.ItemTitle(code)})
		}
		if it.Title == "" {
			it.Title = "FORM " + f.Form
		}
		add(it)
	}
	if prev != nil {
		for _, it := range prev.Items {
			add(it)
		}
	}
	sortItems(s.Items)
	if len(s.Items) == 0 {
		s.Status = StatusEmpty
		s.Absent = &Absent{Reason: fmt.Sprintf("no %s was filed in the window", form)}
	}
	return s, nil
}

// dividend asks whether the tripped session was an ex-dividend date. A raw close
// falls by about the dividend on its ex-date, so the trip can be partly that;
// the trigger does not correct for it (bearing.md #112), the ledger names it.
func (c *Collector) dividend(ctx context.Context, ev trigger.Trip) (*Source, error) {
	if c.Dividends == nil {
		return nil, fmt.Errorf("no dividend source is configured: %w", errNotConfigured)
	}
	d, err := c.Dividends.ExDividend(ctx, ev.Symbol, ev.SessionDate)
	if err != nil {
		return nil, err
	}
	s := &Source{Status: StatusOK, Query: "Yahoo chart, dividend events", Dividend: d}
	if d == nil {
		s.Status = StatusEmpty
		s.Absent = &Absent{Reason: "no ex-dividend date on this session"}
	}
	return s, nil
}

func side(t, closedAt time.Time) string {
	if t.Before(closedAt) {
		return news.SideBefore
	}
	return news.SideAfter
}

func sortItems(items []Item) {
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j].PublishedAt.Before(items[j-1].PublishedAt); j-- {
			items[j], items[j-1] = items[j-1], items[j]
		}
	}
}
