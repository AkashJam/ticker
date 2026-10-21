package fixture

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/AkashJam/ticker/internal/centralbank"
	"github.com/AkashJam/ticker/internal/coverage"
	"github.com/AkashJam/ticker/internal/dividend"
	"github.com/AkashJam/ticker/internal/edgar"
	"github.com/AkashJam/ticker/internal/news"
	"github.com/AkashJam/ticker/internal/reference"
)

// story is what the network would have answered the collector: the one
// invented part of the fixture. It stands in for all four outside sources
// (Google News, the central bank feeds, EDGAR, the dividend source) and is
// keyed the way the collector asks: news by query phrase, banks by feed URL,
// filings by CIK. The headlines and filings are invented; the outlets are real
// names on .example hosts, except where the exclusion list must match a real
// host.
type story struct {
	news      map[string][]news.Item
	bank      map[string][]centralbank.Item
	bankErr   map[string]error
	filings   map[int][]edgar.Filing
	dividends map[string]*dividend.Dividend // by symbol, on its trip session
	divErr    map[string]error
}

// outlet is one outlet's headlines for a source: hours from the close,
// negative before it.
type outlet struct {
	name  string
	host  string // empty: <slug>.example
	hours []float64
}

// A sensor's part of the story. Sources a sensor's routing does not ask are
// never read.
type part struct {
	headline      map[string]string   // economy | sector | topic -> headline
	outlets       map[string][]outlet // economy | sector | topic -> outlets
	bank          *bankItem
	bankFails     bool // the feed answers 403 every time
	filing        *filing
	dividend      *float64 // an ex-dividend on the session, USD per share
	dividendFails bool     // the dividend source answers 429
}

type bankItem struct {
	title    string
	language string
	hours    float64
}

type filing struct {
	form  string
	items []string
	hours float64
}

func usd(v float64) *float64 { return &v }

var parts = map[string]part{
	"MUFG": {
		headline: map[string]string{
			"economy": "Yen slides as the rate outlook shifts",
			"sector":  "Japanese lenders fall with bond yields",
		},
		outlets: map[string][]outlet{
			"economy": {{name: "Nikkei", hours: []float64{-5, 2}}, {name: "Reuters", hours: []float64{-3}}, {name: "Bloomberg", hours: []float64{-1}}, {name: "The Japan Times", hours: []float64{1}}, {name: "Kyodo News", hours: []float64{4}}},
			"sector":  {{name: "Nikkei Asia", hours: []float64{-2}}, {name: "Reuters", hours: []float64{1}}, {name: "Bloomberg", hours: []float64{3}}},
		},
		bank: &bankItem{title: "Summary of Opinions at the Monetary Policy Meeting", language: "en", hours: -6},
	},
	"KB": {
		headline: map[string]string{
			"economy": "Won steadies as exporters buy dollars",
			"sector":  "Korean banks rally on dividend plans",
		},
		outlets: map[string][]outlet{
			"economy": {{name: "Yonhap", hours: []float64{-4}}, {name: "The Korea Herald", hours: []float64{2}}},
			"sector":  {{name: "The Korea Herald", hours: []float64{-3}}, {name: "Yonhap", hours: []float64{-2}}, {name: "JoongAng Daily", hours: []float64{1}}, {name: "Reuters", hours: []float64{5}}},
		},
		filing:   &filing{form: "6-K", hours: 3},
		dividend: usd(0.55),
	},
	"ITUB": {
		headline: map[string]string{
			"economy": "Real weakens on fiscal worries",
			"sector":  "Brazilian bank shares slip",
		},
		outlets: map[string][]outlet{
			"economy": {{name: "Reuters", hours: []float64{-2}}, {name: "Valor International", hours: []float64{1}}, {name: "Bloomberg", hours: []float64{3}}},
			"sector":  {{name: "Reuters", hours: []float64{2}}},
		},
		bank:          &bankItem{title: "BC divulga nota sobre o mercado de câmbio", language: "pt", hours: -3},
		dividendFails: true,
	},
	"CAT": {
		headline: map[string]string{"topic": "Industrial stocks slide on a weak manufacturing survey"},
		outlets: map[string][]outlet{
			"topic": {
				{name: "Reuters", hours: []float64{-3}}, {name: "Barron's", hours: []float64{-1}},
				{name: "IndustryWeek", hours: []float64{2}}, {name: "Global Mining Review", hours: []float64{6}},
				// On the excluded-outlets list by host: counted, never shown.
				{name: "MarketBeat", host: "www.marketbeat.com", hours: []float64{1, 4}},
			},
		},
		filing: &filing{form: "8-K", items: []string{"5.02", "9.01"}, hours: 2.5},
	},
	"CIB": {
		headline: map[string]string{"economy": "Colombian peso firms after the budget vote"},
		outlets: map[string][]outlet{
			"economy": {{name: "Reuters", hours: []float64{-2}}, {name: "Bloomberg Línea", hours: []float64{3}}},
		},
		bankFails: true,
	},
}

// newStory places each picked sensor's part around its trip's close, using the
// same reference configuration the collector reads, so a phrase, feed or CIK
// changed in the reference tables moves the story with it.
func newStory(ctx context.Context, cfg coverage.Config, trips map[string]time.Time, loc *time.Location) (*story, error) {
	st := &story{
		news: map[string][]news.Item{}, bank: map[string][]centralbank.Item{}, bankErr: map[string]error{},
		filings: map[int][]edgar.Filing{}, dividends: map[string]*dividend.Dividend{}, divErr: map[string]error{},
	}
	for sym, session := range trips {
		p, ok := parts[sym]
		if !ok {
			continue // the pending sensor: nothing is ever fetched for it
		}
		sc, err := cfg.Sensor(ctx, sym)
		if err != nil {
			return nil, fmt.Errorf("fixture: story: %w", err)
		}
		closedAt := time.Date(session.Year(), session.Month(), session.Day(), 16, 0, 0, 0, loc).UTC()
		at := func(hours float64) time.Time { return closedAt.Add(time.Duration(hours * float64(time.Hour))) }

		phrases := map[string]string{"economy": sc.EconomyQuery, "sector": sc.SectorQuery, "topic": sc.Topic}
		for kind, outs := range p.outlets {
			phrase := phrases[kind]
			if phrase == "" {
				return nil, fmt.Errorf("fixture: story: %s has %s news but no %s phrase in the reference tables", sym, kind, kind)
			}
			for _, o := range outs {
				host := o.host
				if host == "" {
					host = "www." + slug(o.name) + ".example"
				}
				for i, h := range o.hours {
					st.news[phrase] = append(st.news[phrase], news.Item{
						Title:     fmt.Sprintf("%s - %s", p.headline[kind], o.name),
						Link:      fmt.Sprintf("https://news.google.com/rss/articles/fixture-%s-%s-%s-%d", strings.ToLower(sym), kind, slug(o.name), i),
						Outlet:    o.name,
						OutletURL: "https://" + host,
						Published: at(h),
					})
				}
			}
		}

		if sc.Feed != nil {
			switch {
			case p.bankFails:
				st.bankErr[sc.Feed.URL] = &centralbank.StatusError{Code: 403}
			case p.bank != nil:
				st.bank[sc.Feed.URL] = append(st.bank[sc.Feed.URL], centralbank.Item{
					Title: p.bank.title, Link: strings.TrimSuffix(sc.Feed.URL, "/") + "#fixture-" + strings.ToLower(sym),
					Published: at(p.bank.hours), Language: p.bank.language,
				})
			}
		}

		if p.filing != nil {
			if sc.CIK == 0 {
				return nil, fmt.Errorf("fixture: story: %s files a %s but has no CIK in the edgar dataset", sym, p.filing.form)
			}
			accepted := at(p.filing.hours)
			accession := fmt.Sprintf("0000000000-26-%06d", sc.CIK%1000000)
			st.filings[sc.CIK] = append(st.filings[sc.CIK], edgar.Filing{
				Form: p.filing.form, Accession: accession,
				Filed: accepted.Truncate(24 * time.Hour), Accepted: accepted, Items: p.filing.items,
				Description: "FORM " + p.filing.form, Document: "fixture.htm",
				URL: fmt.Sprintf("https://www.sec.gov/Archives/edgar/data/%d/%s/fixture.htm", sc.CIK, strings.ReplaceAll(accession, "-", "")),
			})
		}

		switch {
		case p.dividendFails:
			st.divErr[sym] = errors.New("dividend: yahoo returned HTTP 429")
		case p.dividend != nil:
			st.dividends[sym] = &dividend.Dividend{Date: session.Format(time.DateOnly), Amount: *p.dividend, Currency: "USD"}
		}
	}
	return st, nil
}

// Search implements coverage.NewsSearcher: the phrase's headlines in [after, before).
func (s *story) Search(_ context.Context, phrase string, after, before time.Time) ([]news.Item, error) {
	var out []news.Item
	for _, it := range s.news[phrase] {
		if !it.Published.Before(after) && it.Published.Before(before) {
			out = append(out, it)
		}
	}
	return out, nil
}

// Fetch implements coverage.BankFetcher: the whole feed, as a real one answers.
func (s *story) Fetch(_ context.Context, feed reference.Feed) ([]centralbank.Item, error) {
	if err := s.bankErr[feed.URL]; err != nil {
		return nil, err
	}
	return s.bank[feed.URL], nil
}

// Recent implements coverage.FilingSource.
func (s *story) Recent(_ context.Context, cik int) ([]edgar.Filing, error) {
	return s.filings[cik], nil
}

// ExDividend implements coverage.DividendSource.
func (s *story) ExDividend(_ context.Context, symbol string, session time.Time) (*dividend.Dividend, error) {
	if err := s.divErr[symbol]; err != nil {
		return nil, err
	}
	d := s.dividends[symbol]
	if d == nil || d.Date != session.Format(time.DateOnly) {
		return nil, nil
	}
	return d, nil
}

func slug(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == ' ' || r == '-':
			b.WriteRune('-')
		}
	}
	return strings.Trim(b.String(), "-")
}
