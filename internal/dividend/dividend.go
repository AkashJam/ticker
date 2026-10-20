// Package dividend asks Yahoo's chart endpoint whether a symbol went ex-dividend
// on a session (bearing.md #112; phase15.md, the 2026-10-20 follow-up). On an
// ex-date the raw close falls by about the dividend, so a trip on that session
// can be partly the dividend. The trigger does not correct for it; the ledger
// names it.
//
// This is a runtime call, unlike internal/backfill, which generates a committed
// SQL file and is never imported by the binary. It runs once per tripped
// sensor (about one call a day), never on a timer, and nothing it returns is a
// price: a failed lookup is a stated absence on the event, not an outage.
package dividend

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const baseURL = "https://query1.finance.yahoo.com/v8/finance/chart/"

// Dividend is one ex-dividend on a session.
type Dividend struct {
	// Date is the ex-date, YYYY-MM-DD in New York.
	Date string `json:"date"`
	// Amount is the dividend per share in Currency, as Yahoo lists it.
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency,omitempty"`
}

// Entry is one dividend in a chart response.
type Entry struct {
	Date   time.Time
	Amount float64
}

// Chart is the part of Yahoo's chart response this package reads.
type Chart struct {
	Currency  string
	Dividends []Entry
}

type chartResponse struct {
	Chart struct {
		Result []struct {
			Meta struct {
				Currency string `json:"currency"`
			} `json:"meta"`
			Events struct {
				Dividends map[string]struct {
					Amount float64 `json:"amount"`
					Date   int64   `json:"date"`
				} `json:"dividends"`
			} `json:"events"`
		} `json:"result"`
		Error *struct {
			Code        string `json:"code"`
			Description string `json:"description"`
		} `json:"error"`
	} `json:"chart"`
}

// Parse decodes one chart response. A response with no events is a chart with no
// dividends, not an error: most sessions have none.
func Parse(r io.Reader) (Chart, error) {
	var resp chartResponse
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		return Chart{}, fmt.Errorf("dividend: decode: %w", err)
	}
	if e := resp.Chart.Error; e != nil {
		return Chart{}, fmt.Errorf("dividend: yahoo error %s: %s", e.Code, e.Description)
	}
	if len(resp.Chart.Result) != 1 {
		return Chart{}, fmt.Errorf("dividend: unexpected response shape")
	}
	res := resp.Chart.Result[0]
	c := Chart{Currency: res.Meta.Currency}
	for _, d := range res.Events.Dividends {
		c.Dividends = append(c.Dividends, Entry{Date: time.Unix(d.Date, 0).UTC(), Amount: d.Amount})
	}
	return c, nil
}

// OnSession returns the dividend whose ex-date falls on session's calendar day in
// loc, or nil when there is none. session is a date (the New York session).
func OnSession(c Chart, session time.Time, loc *time.Location) *Dividend {
	want := session.UTC().Format(time.DateOnly)
	for _, e := range c.Dividends {
		if e.Date.In(loc).Format(time.DateOnly) == want {
			return &Dividend{Date: want, Amount: e.Amount, Currency: c.Currency}
		}
	}
	return nil
}

// Client asks Yahoo.
type Client struct {
	base string
	http *http.Client
	loc  *time.Location
}

// NewClient returns a client for the production endpoint. loc is
// America/New_York, the zone a session and an ex-date are dated in.
func NewClient(loc *time.Location) *Client {
	return &Client{base: baseURL, http: &http.Client{Timeout: 30 * time.Second}, loc: loc}
}

// ExDividend reports the dividend that went ex on session, or nil if none did.
// An error means the lookup failed (a block, a bad body, a timeout), which the
// caller records as an absence and retries; it does not mean there was none.
func (c *Client) ExDividend(ctx context.Context, symbol string, session time.Time) (*Dividend, error) {
	day := session.UTC().Truncate(24 * time.Hour)
	q := url.Values{}
	// A window around the session, so the ex-date is inside it whichever
	// way Yahoo stamps the day; OnSession then picks the exact date.
	q.Set("period1", strconv.FormatInt(day.AddDate(0, 0, -1).Unix(), 10))
	q.Set("period2", strconv.FormatInt(day.AddDate(0, 0, 2).Unix(), 10))
	q.Set("interval", "1d")
	q.Set("events", "div")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+url.PathEscape(symbol)+"?"+q.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("dividend: build request: %w", err)
	}
	// Yahoo answers the default Go user agent with 429 (phase14.md, step 0).
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("dividend: %s: %w", symbol, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dividend: %s: yahoo returned HTTP %d", symbol, resp.StatusCode)
	}
	chart, err := Parse(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", symbol, err)
	}
	return OnSession(chart, session, c.loc), nil
}
