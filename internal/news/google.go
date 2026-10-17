// Package news reads Google News RSS, the keyless geographic and topic query
// behind Phase 15's news (portfolio.md §15; phase8.md "News-source
// verification"). It is undocumented and unofficial, with no service level, and
// every link is a Google redirect wrapper rather than the article's own address.
package news

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const baseURL = "https://news.google.com/rss/search"

// maxBody bounds a response: a hundred items is about 150 KB.
const maxBody = 8 << 20

// Item is one headline.
type Item struct {
	Title     string
	Link      string // a news.google.com redirect wrapper
	Outlet    string // the <source> element
	OutletURL string
	Published time.Time
}

// StatusError is a non-200 response.
type StatusError struct{ Code int }

func (e *StatusError) Error() string { return fmt.Sprintf("news: HTTP %d", e.Code) }

// Client queries Google News RSS.
type Client struct {
	http    *http.Client
	baseURL string
}

func NewClient() *Client {
	return &Client{http: &http.Client{Timeout: 30 * time.Second}, baseURL: baseURL}
}

// Search returns the items for phrase published on or after after and before
// before (Google's `after:` and `before:` operators: whole UTC days, before
// exclusive). Either may be zero to leave that side open. Unlike `when:` the
// window is absolute, so a follow-up or a repair run for an old session asks for
// the same days the first fetch did. The query is the US-English edition.
func (c *Client) Search(ctx context.Context, phrase string, after, before time.Time) ([]Item, error) {
	q := strings.TrimSpace(phrase)
	if q == "" {
		return nil, errors.New("news: empty query")
	}
	if !after.IsZero() {
		q += " after:" + after.UTC().Format(time.DateOnly)
	}
	if !before.IsZero() {
		q += " before:" + before.UTC().Format(time.DateOnly)
	}
	v := url.Values{"q": {q}, "hl": {"en-US"}, "gl": {"US"}, "ceid": {"US:en"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"?"+v.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("news: build request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; akjames.dev-ticker)")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("news: search: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, &StatusError{Code: resp.StatusCode}
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, fmt.Errorf("news: read: %w", err)
	}
	return parse(b)
}

type rss struct {
	Items []struct {
		Title   string `xml:"title"`
		Link    string `xml:"link"`
		PubDate string `xml:"pubDate"`
		Source  struct {
			URL  string `xml:"url,attr"`
			Name string `xml:",chardata"`
		} `xml:"source"`
	} `xml:"channel>item"`
}

func parse(b []byte) ([]Item, error) {
	var doc rss
	if err := xml.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("news: parse feed: %w", err)
	}
	out := make([]Item, 0, len(doc.Items))
	for _, it := range doc.Items {
		item := Item{Title: strings.TrimSpace(it.Title), Link: strings.TrimSpace(it.Link), Outlet: strings.TrimSpace(it.Source.Name), OutletURL: it.Source.URL}
		if t, err := time.Parse(time.RFC1123, strings.TrimSpace(it.PubDate)); err == nil {
			item.Published = t.UTC()
		}
		out = append(out, item)
	}
	return out, nil
}

// DistinctOutlets counts the outlets behind items.
func DistinctOutlets(items []Item) int {
	seen := map[string]struct{}{}
	for _, it := range items {
		if it.Outlet != "" {
			seen[it.Outlet] = struct{}{}
		}
	}
	return len(seen)
}
