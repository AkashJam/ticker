// Package edgar keeps the sensors' SEC identities true (portfolio.md §15
// Phase 15 step 5): ticker to CIK from company_tickers.json, and a drift check
// that each filer is still where the reference table says. EDGAR is keyless but
// asks automated clients to identify themselves, so every request carries a
// User-Agent with a real contact (FETCH_CONTACT) and stays under ten requests a
// second.
package edgar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	tickersURL     = "https://www.sec.gov/files/company_tickers.json"
	submissionsURL = "https://data.sec.gov/submissions"

	// maxBody bounds a response. company_tickers.json is about 1 MB.
	maxBody = 16 << 20

	// requestGap keeps the client at about eight requests a second, under
	// SEC's ten.
	requestGap = 125 * time.Millisecond
)

// ErrNoContact means no contact was supplied: without one the User-Agent would
// not identify us, and SEC throttles or blocks such clients.
var ErrNoContact = errors.New("edgar: FETCH_CONTACT is required")

// StatusError is a non-200 response.
type StatusError struct {
	Code       int
	RetryAfter time.Duration
}

func (e *StatusError) Error() string { return fmt.Sprintf("edgar: HTTP %d", e.Code) }

// Retryable reports whether trying the same call again can succeed.
func (e *StatusError) Retryable() bool {
	return e.Code == http.StatusTooManyRequests || e.Code >= 500
}

// TickerRow is one company_tickers.json entry.
type TickerRow struct {
	CIK   int
	Title string
}

// Submission is the part of a submissions record the drift check reads.
type Submission struct {
	CIK             int
	Name            string
	Tickers         []string
	BusinessCountry string // addresses.business.stateOrCountryDescription
}

// Client calls EDGAR. It is safe for sequential use; it paces its own calls.
type Client struct {
	http           *http.Client
	userAgent      string
	tickersURL     string
	submissionsURL string
	gap            time.Duration
	sleep          func(ctx context.Context, d time.Duration) error
	last           time.Time
	now            func() time.Time
}

// NewClient returns a client that identifies itself with contact. It rejects an
// empty contact and anything that could break out of a header.
func NewClient(contact string) (*Client, error) {
	contact = strings.TrimSpace(contact)
	if contact == "" {
		return nil, ErrNoContact
	}
	for _, r := range contact {
		if unicode.IsControl(r) {
			return nil, errors.New("edgar: FETCH_CONTACT contains a control character")
		}
	}
	return &Client{
		http:           &http.Client{Timeout: 30 * time.Second},
		userAgent:      "akjames.dev-ticker " + contact,
		tickersURL:     tickersURL,
		submissionsURL: submissionsURL,
		gap:            requestGap,
		sleep:          sleepCtx,
		now:            time.Now,
	}, nil
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

func (c *Client) get(ctx context.Context, url string) ([]byte, error) {
	if wait := c.gap - c.now().Sub(c.last); wait > 0 {
		if err := c.sleep(ctx, wait); err != nil {
			return nil, err
		}
	}
	c.last = c.now()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("edgar: build request: %w", err)
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("edgar: get %s: %w", urlPath(url), err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		se := &StatusError{Code: resp.StatusCode}
		if secs, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && secs > 0 {
			se.RetryAfter = time.Duration(secs) * time.Second
		}
		return nil, se
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, fmt.Errorf("edgar: read %s: %w", urlPath(url), err)
	}
	return b, nil
}

func urlPath(u string) string {
	if i := strings.Index(u, "://"); i >= 0 {
		u = u[i+3:]
	}
	return u
}

// Tickers returns company_tickers.json keyed by upper-case ticker.
func (c *Client) Tickers(ctx context.Context) (map[string]TickerRow, error) {
	b, err := c.get(ctx, c.tickersURL)
	if err != nil {
		return nil, err
	}
	var raw map[string]struct {
		CIK    int    `json:"cik_str"`
		Ticker string `json:"ticker"`
		Title  string `json:"title"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("edgar: parse company_tickers.json: %w", err)
	}
	if len(raw) == 0 {
		return nil, errors.New("edgar: company_tickers.json is empty")
	}
	out := make(map[string]TickerRow, len(raw))
	for _, r := range raw {
		out[strings.ToUpper(r.Ticker)] = TickerRow{CIK: r.CIK, Title: r.Title}
	}
	return out, nil
}

// Submissions returns the filer's record for cik.
func (c *Client) Submissions(ctx context.Context, cik int) (Submission, error) {
	b, err := c.get(ctx, fmt.Sprintf("%s/CIK%010d.json", c.submissionsURL, cik))
	if err != nil {
		return Submission{}, err
	}
	var raw struct {
		CIK       string   `json:"cik"`
		Name      string   `json:"name"`
		Tickers   []string `json:"tickers"`
		Addresses struct {
			Business struct {
				StateOrCountryDescription string `json:"stateOrCountryDescription"`
			} `json:"business"`
		} `json:"addresses"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return Submission{}, fmt.Errorf("edgar: parse submissions for CIK %d: %w", cik, err)
	}
	return Submission{
		CIK: cik, Name: raw.Name, Tickers: raw.Tickers,
		BusinessCountry: raw.Addresses.Business.StateOrCountryDescription,
	}, nil
}
