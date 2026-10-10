// Package fred fetches the two per-country index series Atlas needs (portfolio.md
// §15 Phase 10): OECD share prices and BIS real residential property prices.
// It reads FRED's keyless fredgraph.csv endpoint, so it needs no secret.
package fred

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://fred.stlouisfed.org/graph"

// Obs is one observation. Dates are the first day of the month (monthly
// series) or quarter (quarterly series), as FRED publishes them.
type Obs struct {
	Date  time.Time
	Value float64
}

// ErrNoData means FRED answered but the series has no usable observation.
var ErrNoData = errors.New("fred: series has no data")

// StatusError is a non-200 response. FRED answers 400 for an unknown series.
type StatusError struct {
	Code       int
	RetryAfter time.Duration
}

func (e *StatusError) Error() string { return fmt.Sprintf("fred: HTTP %d", e.Code) }

// Retryable reports whether trying the same call again can succeed.
func (e *StatusError) Retryable() bool {
	return e.Code == http.StatusTooManyRequests || e.Code >= 500
}

// Client reads series from fredgraph.csv.
type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient() *Client {
	return &Client{baseURL: baseURL, http: &http.Client{Timeout: 20 * time.Second}}
}

// Series returns every observation of id in date order. Missing values, which
// FRED writes as ".", are dropped rather than invented.
func (c *Client) Series(ctx context.Context, id string) ([]Obs, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/fredgraph.csv?id="+url.QueryEscape(id), nil)
	if err != nil {
		return nil, fmt.Errorf("fred: build request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fred: series %s: %w", id, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		se := &StatusError{Code: resp.StatusCode}
		if secs, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && secs > 0 {
			se.RetryAfter = time.Duration(secs) * time.Second
		}
		return nil, se
	}
	obs, err := parseCSV(resp.Body, id)
	if err != nil {
		return nil, err
	}
	if len(obs) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNoData, id)
	}
	return obs, nil
}

func parseCSV(r io.Reader, id string) ([]Obs, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("fred: parse %s: %w", id, err)
	}
	if len(rows) == 0 || len(rows[0]) != 2 || !strings.EqualFold(rows[0][1], id) {
		return nil, fmt.Errorf("fred: %s: unexpected header", id)
	}
	obs := make([]Obs, 0, len(rows)-1)
	for _, row := range rows[1:] {
		if row[1] == "." || row[1] == "" {
			continue
		}
		d, err := time.Parse(time.DateOnly, row[0])
		if err != nil {
			return nil, fmt.Errorf("fred: %s: bad date %q", id, row[0])
		}
		v, err := strconv.ParseFloat(row[1], 64)
		if err != nil {
			return nil, fmt.Errorf("fred: %s: bad value %q", id, row[1])
		}
		obs = append(obs, Obs{Date: d, Value: v})
	}
	return obs, nil
}
