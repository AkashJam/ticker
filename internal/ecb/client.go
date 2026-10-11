// Package ecb fetches the ECB euro foreign exchange reference rates (portfolio.md
// §15 Phase 10): the history zip once, then the daily XML. Both are keyless.
// Rates are units of the quoted currency per €1.
package ecb

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"time"
)

const baseURL = "https://www.ecb.europa.eu/stats/eurofxref"

// maxBody bounds a response: the history zip is about 640 KB.
const maxBody = 10 << 20

// Day is one publication: the rates the ECB set for a business day.
type Day struct {
	Date  time.Time
	Rates map[string]float64
}

// ErrNoData means the ECB answered but the file held no usable rate.
var ErrNoData = errors.New("ecb: no data")

// StatusError is a non-200 response.
type StatusError struct {
	Code       int
	RetryAfter time.Duration
}

func (e *StatusError) Error() string { return fmt.Sprintf("ecb: HTTP %d", e.Code) }

// Retryable reports whether trying the same call again can succeed.
func (e *StatusError) Retryable() bool {
	return e.Code == http.StatusTooManyRequests || e.Code >= 500
}

// Client downloads the ECB reference rate files.
type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient() *Client {
	return &Client{baseURL: baseURL, http: &http.Client{Timeout: 30 * time.Second}}
}

func (c *Client) get(ctx context.Context, name string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/"+name, nil)
	if err != nil {
		return nil, fmt.Errorf("ecb: build request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ecb: get %s: %w", name, err)
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
		return nil, fmt.Errorf("ecb: read %s: %w", name, err)
	}
	return b, nil
}

// History returns every published day since 1999, oldest first.
func (c *Client) History(ctx context.Context) ([]Day, error) {
	b, err := c.get(ctx, "eurofxref-hist.zip")
	if err != nil {
		return nil, err
	}
	return parseHistory(b)
}

// Daily returns the latest published day.
func (c *Client) Daily(ctx context.Context) (Day, error) {
	b, err := c.get(ctx, "eurofxref-daily.xml")
	if err != nil {
		return Day{}, err
	}
	return parseDaily(b)
}

func parseHistory(b []byte) ([]Day, error) {
	zr, err := zip.NewReader(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		return nil, fmt.Errorf("ecb: open history zip: %w", err)
	}
	if len(zr.File) == 0 {
		return nil, fmt.Errorf("%w: empty history zip", ErrNoData)
	}
	f, err := zr.File[0].Open()
	if err != nil {
		return nil, fmt.Errorf("ecb: open history csv: %w", err)
	}
	defer func() { _ = f.Close() }()

	r := csv.NewReader(io.LimitReader(f, maxBody*4))
	r.FieldsPerRecord = -1 // rows carry a trailing empty column
	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("ecb: parse history csv: %w", err)
	}
	if len(rows) < 2 || len(rows[0]) < 2 || rows[0][0] != "Date" {
		return nil, fmt.Errorf("ecb: history csv: unexpected header")
	}
	head := rows[0]
	days := make([]Day, 0, len(rows)-1)
	for _, row := range rows[1:] {
		date, err := time.Parse(time.DateOnly, row[0])
		if err != nil {
			return nil, fmt.Errorf("ecb: history csv: bad date %q", row[0])
		}
		d := Day{Date: date, Rates: map[string]float64{}}
		for i := 1; i < len(row) && i < len(head); i++ {
			if head[i] == "" || row[i] == "" || row[i] == "N/A" {
				continue
			}
			v, err := strconv.ParseFloat(row[i], 64)
			if err != nil || v <= 0 {
				return nil, fmt.Errorf("ecb: history csv: bad rate %q for %s on %s", row[i], head[i], row[0])
			}
			d.Rates[head[i]] = v
		}
		days = append(days, d)
	}
	if len(days) == 0 {
		return nil, fmt.Errorf("%w: history has no rows", ErrNoData)
	}
	slices.Reverse(days) // the file is newest first
	return days, nil
}

func parseDaily(b []byte) (Day, error) {
	var doc struct {
		Days []struct {
			Time  string `xml:"time,attr"`
			Rates []struct {
				Currency string  `xml:"currency,attr"`
				Rate     float64 `xml:"rate,attr"`
			} `xml:"Cube"`
		} `xml:"Cube>Cube"`
	}
	if err := xml.Unmarshal(b, &doc); err != nil {
		return Day{}, fmt.Errorf("ecb: parse daily xml: %w", err)
	}
	if len(doc.Days) != 1 {
		return Day{}, fmt.Errorf("%w: daily xml has %d days", ErrNoData, len(doc.Days))
	}
	date, err := time.Parse(time.DateOnly, doc.Days[0].Time)
	if err != nil {
		return Day{}, fmt.Errorf("ecb: daily xml: bad date %q", doc.Days[0].Time)
	}
	d := Day{Date: date, Rates: map[string]float64{}}
	for _, r := range doc.Days[0].Rates {
		if r.Currency != "" && r.Rate > 0 {
			d.Rates[r.Currency] = r.Rate
		}
	}
	if len(d.Rates) == 0 {
		return Day{}, fmt.Errorf("%w: daily xml has no rates", ErrNoData)
	}
	return d, nil
}
