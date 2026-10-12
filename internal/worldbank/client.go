// Package worldbank fetches the national series Atlas draws from the World Bank
// API (portfolio.md §15 Phase 10): the CPI series behind the prices measure, the
// level measures, and the economic composition. It is keyless.
package worldbank

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.worldbank.org/v2"

// maxBody bounds a response; the largest (a CPI series for 23 countries) is
// well under 1 MB.
const maxBody = 8 << 20

// Obs is one published value for one country and year.
type Obs struct {
	Code  string // ISO 3166 alpha-2
	Year  int
	Value float64
}

// ErrBadBody is an empty, truncated or unparsable response. The API returns
// these intermittently with HTTP 200, and the next attempt succeeds, so it is
// retryable.
var ErrBadBody = errors.New("worldbank: empty or malformed response")

// ErrNoData means the API answered but held no value for any country.
var ErrNoData = errors.New("worldbank: no data")

// APIError is the API's own error message (an unknown indicator, a bad
// parameter). Retrying cannot help.
type APIError struct{ Message string }

func (e *APIError) Error() string { return "worldbank: " + e.Message }

// StatusError is a non-200 response.
type StatusError struct {
	Code       int
	RetryAfter time.Duration
}

func (e *StatusError) Error() string { return fmt.Sprintf("worldbank: HTTP %d", e.Code) }

// Retryable reports whether trying the same call again can succeed.
func (e *StatusError) Retryable() bool {
	return e.Code == http.StatusTooManyRequests || e.Code >= 500
}

// Client calls the World Bank indicators API.
type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient() *Client {
	return &Client{baseURL: baseURL, http: &http.Client{Timeout: 30 * time.Second}}
}

type page struct {
	pages int
	obs   []Obs
}

// Indicator returns the values of indicator id for the given countries. query
// carries mrnev=1 (the most recent non-empty value) or a date range. Null
// values are dropped, never filled.
func (c *Client) Indicator(ctx context.Context, codes []string, id string, query url.Values) ([]Obs, error) {
	var all []Obs
	for p := 1; ; p++ {
		q := url.Values{"format": {"json"}, "per_page": {"1000"}, "page": {strconv.Itoa(p)}}
		for k, v := range query {
			q[k] = v
		}
		u := fmt.Sprintf("%s/country/%s/indicator/%s?%s", c.baseURL, strings.Join(codes, ";"), url.PathEscape(id), q.Encode())
		pg, err := c.get(ctx, u, id)
		if err != nil {
			return nil, err
		}
		all = append(all, pg.obs...)
		if p >= pg.pages {
			break
		}
	}
	if len(all) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNoData, id)
	}
	return all, nil
}

func (c *Client) get(ctx context.Context, u, id string) (page, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return page{}, fmt.Errorf("worldbank: build request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return page{}, fmt.Errorf("worldbank: %s: %w", id, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		se := &StatusError{Code: resp.StatusCode}
		if secs, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && secs > 0 {
			se.RetryAfter = time.Duration(secs) * time.Second
		}
		return page{}, se
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return page{}, fmt.Errorf("%w: %s: %w", ErrBadBody, id, err)
	}
	return parse(b, id)
}

func parse(b []byte, id string) (page, error) {
	var top []json.RawMessage
	if err := json.Unmarshal(b, &top); err != nil || len(top) == 0 {
		return page{}, fmt.Errorf("%w: %s", ErrBadBody, id)
	}
	if len(top) == 1 { // {"message":[{"id":…,"value":…}]}
		var e struct {
			Message []struct {
				ID    string `json:"id"`
				Value string `json:"value"`
			} `json:"message"`
		}
		if err := json.Unmarshal(top[0], &e); err == nil && len(e.Message) > 0 {
			return page{}, &APIError{Message: fmt.Sprintf("%s: %s %s", id, e.Message[0].ID, e.Message[0].Value)}
		}
		return page{}, fmt.Errorf("%w: %s", ErrBadBody, id)
	}
	var meta struct {
		Pages int `json:"pages"`
	}
	var rows []struct {
		Country struct {
			ID string `json:"id"`
		} `json:"country"`
		Date  string   `json:"date"`
		Value *float64 `json:"value"`
	}
	if err := json.Unmarshal(top[0], &meta); err != nil {
		return page{}, fmt.Errorf("%w: %s", ErrBadBody, id)
	}
	if err := json.Unmarshal(top[1], &rows); err != nil {
		return page{}, fmt.Errorf("%w: %s", ErrBadBody, id)
	}
	pg := page{pages: meta.Pages}
	for _, r := range rows {
		year, err := strconv.Atoi(r.Date)
		if r.Value == nil || err != nil {
			continue
		}
		pg.obs = append(pg.obs, Obs{Code: r.Country.ID, Year: year, Value: *r.Value})
	}
	return pg, nil
}
