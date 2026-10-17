// Package centralbank reads the central banks' press-release feeds (portfolio.md
// §15 Phase 15 step 6; phase8.md "News-source verification"). Eight of the
// thirteen country sensors have one, in three formats: RSS 2.0, RSS 1.0 (the Bank
// of Canada, with its cb: namespace) and Atom (the Brazilian and Turkish banks).
// Their dates are as varied: RFC 3339, RFC 1123 with and without an offset, and
// the Turkish bank's "Oct 9, 2026, 12:00:00 AM".
package centralbank

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/AkashJam/ticker/internal/reference"
)

// maxBody bounds a response: the largest feed seen is 95 KB.
const maxBody = 8 << 20

// Item is one press release.
type Item struct {
	Title     string
	Link      string
	Published time.Time
	Language  string // the feed's, ISO 639-1
}

// StatusError is a non-200 response.
type StatusError struct{ Code int }

func (e *StatusError) Error() string { return fmt.Sprintf("centralbank: HTTP %d", e.Code) }

// Client fetches feeds.
type Client struct{ http *http.Client }

func NewClient() *Client { return &Client{http: &http.Client{Timeout: 30 * time.Second}} }

// Fetch reads and parses feed. The bank's site sees an ordinary client, not a
// scraper: three of the eight answer only to something that looks like a browser.
func (c *Client) Fetch(ctx context.Context, feed reference.Feed) ([]Item, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, feed.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("centralbank: build request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; akjames.dev-ticker)")
	req.Header.Set("Accept", "application/rss+xml, application/atom+xml, application/xml, text/xml, */*")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("centralbank: get %s: %w", feed.URL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, &StatusError{Code: resp.StatusCode}
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, fmt.Errorf("centralbank: read %s: %w", feed.URL, err)
	}
	var loc *time.Location
	if feed.Timezone != "" {
		if loc, err = time.LoadLocation(feed.Timezone); err != nil {
			return nil, fmt.Errorf("centralbank: timezone %q: %w", feed.Timezone, err)
		}
	}
	return Parse(feed.Format, feed.Language, b, loc)
}

type rawItem struct {
	Title     string `xml:"title"`
	PubDate   string `xml:"pubDate"`
	DCDate    string `xml:"date"` // dc:date, whatever prefix the feed binds
	Published string `xml:"published"`
	Updated   string `xml:"updated"`
	Links     []struct {
		Rel  string `xml:"rel,attr"`
		Href string `xml:"href,attr"`
		Text string `xml:",chardata"`
	} `xml:"link"`
}

type document struct {
	Channel struct {
		Items []rawItem `xml:"item"`
	} `xml:"channel"`
	RootItems []rawItem `xml:"item"` // RSS 1.0 puts items beside the channel
	Entries   []rawItem `xml:"entry"`
}

// Parse reads a feed in the given format ("rss2", "rss1" or "atom"). loc is the
// zone of dates that carry no offset; nil means UTC. An item with no usable
// title, link or date is dropped, never guessed.
func Parse(format, language string, body []byte, loc *time.Location) ([]Item, error) {
	body = []byte(strings.TrimPrefix(string(body), "\uFEFF"))
	var doc document
	if err := xml.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("centralbank: parse %s feed: %w", format, err)
	}
	var raw []rawItem
	switch format {
	case "rss2":
		raw = doc.Channel.Items
	case "rss1":
		raw = doc.RootItems
	case "atom":
		raw = doc.Entries
	default:
		return nil, fmt.Errorf("centralbank: unknown format %q", format)
	}
	if loc == nil {
		loc = time.UTC
	}
	out := make([]Item, 0, len(raw))
	for _, r := range raw {
		it := Item{Title: clean(r.Title), Link: link(r), Language: language}
		for _, d := range []string{r.PubDate, r.DCDate, r.Published, r.Updated} {
			if t, ok := ParseDate(d, loc); ok {
				it.Published = t
				break
			}
		}
		if it.Title == "" || it.Link == "" || it.Published.IsZero() {
			continue
		}
		out = append(out, it)
	}
	if len(raw) > 0 && len(out) == 0 {
		return nil, errors.New("centralbank: no item had a title, a link and a date")
	}
	return out, nil
}

func clean(s string) string { return strings.Join(strings.Fields(s), " ") }

func link(r rawItem) string {
	for _, l := range r.Links {
		if l.Rel != "" && l.Rel != "alternate" {
			continue
		}
		if h := strings.TrimSpace(l.Href); h != "" {
			return h
		}
		if t := strings.TrimSpace(l.Text); t != "" {
			return t
		}
	}
	return ""
}

var dateLayouts = []struct {
	layout string
	hasTZ  bool
}{
	{time.RFC3339, true},
	{time.RFC1123Z, true},
	{time.RFC1123, true},
	{"Mon, 02 Jan 2006 15:04:05", false}, // the RBI: no offset
	{"Jan 2, 2006, 3:04:05 PM", false},   // the Turkish bank
	{"2006-01-02T15:04:05", false},
}

// ParseDate reads a feed date; loc is the zone of one that has no offset.
func ParseDate(s string, loc *time.Location) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	for _, l := range dateLayouts {
		var t time.Time
		var err error
		if l.hasTZ {
			t, err = time.Parse(l.layout, s)
		} else {
			t, err = time.ParseInLocation(l.layout, s, loc)
		}
		if err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// InWindow keeps the items published in [from, to), newest first.
func InWindow(items []Item, from, to time.Time) []Item {
	var out []Item
	for _, it := range items {
		if !it.Published.Before(from) && it.Published.Before(to) {
			out = append(out, it)
		}
	}
	slices.SortFunc(out, func(a, b Item) int { return b.Published.Compare(a.Published) })
	return out
}
