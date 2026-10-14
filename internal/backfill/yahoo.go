// Package backfill builds migration 000006: the one-time load of daily bars
// from Yahoo's chart endpoint for the sweep universe (portfolio.md ADR-009).
// It is a generator for a committed SQL file, not a runtime dependency: the
// ticker binary never imports it, so Yahoo stays out of production (§7).
package backfill

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

const yahooBaseURL = "https://query1.finance.yahoo.com/v8/finance/chart/"

// Chart is the part of Yahoo's chart response the backfill reads. Every
// price array can hold nulls (a day Yahoo has a timestamp for but no print).
type Chart struct {
	Symbol    string
	Timestamp []int64
	Open      []*float64
	High      []*float64
	Low       []*float64
	Close     []*float64
	AdjClose  []*float64
	Dividends []int64 // ex-date timestamps
	Splits    []int64
}

type chartResponse struct {
	Chart struct {
		Result []struct {
			Timestamp  []int64 `json:"timestamp"`
			Indicators struct {
				Quote []struct {
					Open  []*float64 `json:"open"`
					High  []*float64 `json:"high"`
					Low   []*float64 `json:"low"`
					Close []*float64 `json:"close"`
				} `json:"quote"`
				AdjClose []struct {
					AdjClose []*float64 `json:"adjclose"`
				} `json:"adjclose"`
			} `json:"indicators"`
			Events struct {
				Dividends map[string]struct {
					Date int64 `json:"date"`
				} `json:"dividends"`
				Splits map[string]struct {
					Date int64 `json:"date"`
				} `json:"splits"`
			} `json:"events"`
		} `json:"result"`
		Error *struct {
			Code        string `json:"code"`
			Description string `json:"description"`
		} `json:"error"`
	} `json:"chart"`
}

// ParseChart decodes one chart response. It rejects arrays of unequal
// length, which would silently misalign a date with a price.
func ParseChart(symbol string, r io.Reader) (Chart, error) {
	var resp chartResponse
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		return Chart{}, fmt.Errorf("backfill: %s: decode: %w", symbol, err)
	}
	if e := resp.Chart.Error; e != nil {
		return Chart{}, fmt.Errorf("backfill: %s: yahoo error %s: %s", symbol, e.Code, e.Description)
	}
	if len(resp.Chart.Result) != 1 || len(resp.Chart.Result[0].Indicators.Quote) != 1 {
		return Chart{}, fmt.Errorf("backfill: %s: unexpected response shape", symbol)
	}
	res := resp.Chart.Result[0]
	q := res.Indicators.Quote[0]
	c := Chart{Symbol: symbol, Timestamp: res.Timestamp, Open: q.Open, High: q.High, Low: q.Low, Close: q.Close}
	if len(res.Indicators.AdjClose) == 1 {
		c.AdjClose = res.Indicators.AdjClose[0].AdjClose
	}
	for _, d := range res.Events.Dividends {
		c.Dividends = append(c.Dividends, d.Date)
	}
	for _, s := range res.Events.Splits {
		c.Splits = append(c.Splits, s.Date)
	}
	n := len(c.Timestamp)
	for name, l := range map[string]int{"open": len(c.Open), "high": len(c.High), "low": len(c.Low), "close": len(c.Close), "adjclose": len(c.AdjClose)} {
		if l != n {
			return Chart{}, fmt.Errorf("backfill: %s: %s has %d values for %d timestamps", symbol, name, l, n)
		}
	}
	return c, nil
}

// Client fetches charts from Yahoo.
type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient() *Client {
	return &Client{baseURL: yahooBaseURL, http: &http.Client{Timeout: 20 * time.Second}}
}

// Fetch asks for daily bars in [from, to). Explicit period1/period2 rather
// than range=, so a re-run next month produces the same file.
func (c *Client) Fetch(ctx context.Context, symbol string, from, to time.Time) (Chart, error) {
	q := url.Values{}
	q.Set("period1", strconv.FormatInt(from.Unix(), 10))
	q.Set("period2", strconv.FormatInt(to.Unix(), 10))
	q.Set("interval", "1d")
	q.Set("events", "div|split")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+url.PathEscape(symbol)+"?"+q.Encode(), nil)
	if err != nil {
		return Chart{}, err
	}
	// Yahoo answers the default Go user agent with 429.
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := c.http.Do(req)
	if err != nil {
		return Chart{}, fmt.Errorf("backfill: %s: %w", symbol, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return Chart{}, fmt.Errorf("backfill: %s: yahoo returned HTTP %d", symbol, resp.StatusCode)
	}
	return ParseChart(symbol, resp.Body)
}
