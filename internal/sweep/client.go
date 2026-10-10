// Package sweep is the post-close daily-bar job (portfolio.md §15 Phase 8):
// one /quote snapshot per symbol after the US close, written as that
// session's "1d" bar. Daily bars cannot be backfilled on Finnhub's free tier
// (/stock/candle is 403), so every session this does not run is lost for good.
package sweep

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const finnhubBaseURL = "https://finnhub.io/api/v1"

// Quote is the part of Finnhub's /quote response the sweep uses. /quote has
// no volume, which is why a swept bar carries none.
type Quote struct {
	Open, High, Low, Current, PrevClose float64
	// At is the timestamp of the last trade (Finnhub's `t`). After the close
	// it is the closing print, so its New York date is the session date.
	At time.Time
}

// Quoter is the one call the runner needs, so tests can fake Finnhub.
type Quoter interface {
	Quote(ctx context.Context, symbol string) (Quote, error)
}

// ErrNoData is Finnhub's answer for a symbol it doesn't know: HTTP 200 with
// every field zero.
var ErrNoData = errors.New("sweep: quote has no data")

// StatusError is a non-200 response. 401 and 403 are not worth retrying.
type StatusError struct {
	Code       int
	RetryAfter time.Duration // set on 429 when the server says how long
}

func (e *StatusError) Error() string { return fmt.Sprintf("sweep: finnhub returned HTTP %d", e.Code) }

// Retryable reports whether trying the same call again can succeed.
func (e *StatusError) Retryable() bool {
	return e.Code == http.StatusTooManyRequests || e.Code >= 500
}

// FinnhubClient calls Finnhub's REST API. The key travels in the
// X-Finnhub-Token header, never the URL, so no error or log line can carry it.
type FinnhubClient struct {
	baseURL string
	key     string
	http    *http.Client
}

func NewFinnhubClient(key string) *FinnhubClient {
	return &FinnhubClient{baseURL: finnhubBaseURL, key: key, http: &http.Client{Timeout: 10 * time.Second}}
}

func (c *FinnhubClient) Quote(ctx context.Context, symbol string) (Quote, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/quote?symbol="+url.QueryEscape(symbol), nil)
	if err != nil {
		return Quote{}, fmt.Errorf("sweep: build request: %w", err)
	}
	req.Header.Set("X-Finnhub-Token", c.key)

	resp, err := c.http.Do(req)
	if err != nil {
		return Quote{}, fmt.Errorf("sweep: quote %s: %w", symbol, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		se := &StatusError{Code: resp.StatusCode}
		if secs, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && secs > 0 {
			se.RetryAfter = time.Duration(secs) * time.Second
		}
		return Quote{}, se
	}

	var body struct {
		C  float64 `json:"c"`
		H  float64 `json:"h"`
		L  float64 `json:"l"`
		O  float64 `json:"o"`
		PC float64 `json:"pc"`
		T  int64   `json:"t"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return Quote{}, fmt.Errorf("sweep: decode quote %s: %w", symbol, err)
	}
	if body.T == 0 || body.C == 0 {
		return Quote{}, fmt.Errorf("%w: %s", ErrNoData, symbol)
	}
	return Quote{Open: body.O, High: body.H, Low: body.L, Current: body.C, PrevClose: body.PC, At: time.Unix(body.T, 0).UTC()}, nil
}
