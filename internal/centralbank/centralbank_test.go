package centralbank

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/reference"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func utc(y int, m time.Month, d, h, mi, s int) time.Time {
	return time.Date(y, m, d, h, mi, s, 0, time.UTC)
}

// The fixtures are the first three items of the real feeds, fetched 17 Oct 2026.
func TestParseTheEightRealFeeds(t *testing.T) {
	ist, _ := time.LoadLocation("Asia/Kolkata")
	trt, _ := time.LoadLocation("Europe/Istanbul")
	for _, tt := range []struct {
		name, format, lang string
		loc                *time.Location
		wantFirst          time.Time
	}{
		{"rss1_bankofcanada.xml", "rss1", "en", nil, utc(2026, 9, 10, 11, 0, 9)},
		{"atom_bcb.xml", "atom", "pt", nil, utc(2026, 10, 6, 10, 34, 47)},   // 07:34:47-03:00; the feed-level updated stamp is not an item
		{"rss2_banrep.xml", "rss2", "es", nil, utc(2021, 11, 5, 17, 2, 22)}, // the feed is not newest-first
		{"rss2_boe.xml", "rss2", "en", nil, utc(2026, 10, 16, 11, 0, 0)},    // 12:00 +0100
		{"rss2_boj.xml", "rss2", "en", nil, utc(2026, 10, 16, 8, 0, 0)},     // 17:00 +0900
		{"rss2_rbi.xml", "rss2", "en", ist, utc(2026, 10, 16, 13, 0, 0)},    // 18:30 IST, no offset in the feed
		{"atom_tcmb.xml", "atom", "en", trt, utc(2026, 10, 8, 21, 0, 0)},    // Oct 9, 12:00 AM TRT
		{"rss2_ecb.xml", "rss2", "en", nil, utc(2026, 10, 17, 5, 0, 0)},     // 07:00 +0200
	} {
		t.Run(tt.name, func(t *testing.T) {
			items, err := Parse(tt.format, tt.lang, fixture(t, tt.name), tt.loc)
			if err != nil {
				t.Fatal(err)
			}
			if len(items) != 3 {
				t.Fatalf("%d items, want 3", len(items))
			}
			first := items[0]
			if first.Title == "" || first.Link == "" || first.Language != tt.lang {
				t.Errorf("first = %+v", first)
			}
			if !first.Published.Equal(tt.wantFirst) {
				t.Errorf("published %s, want %s", first.Published, tt.wantFirst)
			}
			for _, it := range items {
				if it.Published.IsZero() || it.Link == "" {
					t.Errorf("incomplete item %+v", it)
				}
			}
		})
	}
}

func TestParseDate(t *testing.T) {
	ist, _ := time.LoadLocation("Asia/Kolkata")
	for _, tt := range []struct {
		in   string
		loc  *time.Location
		want time.Time
		ok   bool
	}{
		{"2026-10-16T23:19:41+00:00", nil, utc(2026, 10, 16, 23, 19, 41), true},
		{"Fri, 16 Oct 2026 12:00:00 +0100", nil, utc(2026, 10, 16, 11, 0, 0), true},
		{"Fri, 16 Oct 2026 18:30:00", ist, utc(2026, 10, 16, 13, 0, 0), true},
		{"Fri, 16 Oct 2026 18:30:00", nil, utc(2026, 10, 16, 18, 30, 0), true}, // no zone known: UTC
		{"Oct 9, 2026, 9:26:39 PM", nil, utc(2026, 10, 9, 21, 26, 39), true},
		{"", nil, time.Time{}, false},
		{"yesterday", nil, time.Time{}, false},
	} {
		got, ok := ParseDate(tt.in, func() *time.Location {
			if tt.loc == nil {
				return time.UTC
			}
			return tt.loc
		}())
		if ok != tt.ok || (ok && !got.Equal(tt.want)) {
			t.Errorf("ParseDate(%q) = %s, %v; want %s, %v", tt.in, got, ok, tt.want, tt.ok)
		}
	}
}

func TestParseDropsItemsItCannotUseAndRefusesAWrongFormat(t *testing.T) {
	const doc = `<rss><channel>
<item><title>ok</title><link>https://x/1</link><pubDate>Fri, 16 Oct 2026 12:00:00 +0000</pubDate></item>
<item><title>no date</title><link>https://x/2</link></item>
<item><title></title><link>https://x/3</link><pubDate>Fri, 16 Oct 2026 12:00:00 +0000</pubDate></item>
</channel></rss>`
	items, err := Parse("rss2", "en", []byte(doc), nil)
	if err != nil || len(items) != 1 || items[0].Title != "ok" {
		t.Fatalf("items %+v, err %v; only the complete item may survive", items, err)
	}
	if _, err := Parse("rss2", "en", []byte(`<rss><channel><item><title>x</title></item></channel></rss>`), nil); err == nil {
		t.Error("a feed whose every item is unusable must be an error, not an empty success")
	}
	if _, err := Parse("json", "en", []byte(doc), nil); err == nil {
		t.Error("an unknown format must be refused")
	}
	if _, err := Parse("rss2", "en", []byte("<html>blocked</html>"), nil); err != nil {
		t.Logf("a bot-block page parses to %v", err)
	}
	if items, err := Parse("rss2", "en", []byte(`<rss><channel></channel></rss>`), nil); err != nil || len(items) != 0 {
		t.Errorf("an empty feed is an empty result: %v %v", items, err)
	}
}

func TestInWindowKeepsTheWindowNewestFirst(t *testing.T) {
	items := []Item{
		{Title: "old", Published: utc(2026, 10, 1, 0, 0, 0)},
		{Title: "a", Published: utc(2026, 10, 15, 10, 0, 0)},
		{Title: "b", Published: utc(2026, 10, 16, 10, 0, 0)},
		{Title: "to", Published: utc(2026, 10, 18, 0, 0, 0)}, // the end is exclusive
	}
	got := InWindow(items, utc(2026, 10, 14, 0, 0, 0), utc(2026, 10, 18, 0, 0, 0))
	if len(got) != 2 || got[0].Title != "b" || got[1].Title != "a" {
		t.Errorf("got %+v", got)
	}
}

func TestFetch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/blocked" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.Header.Get("User-Agent") == "" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=UTF-8") // as the Turkish bank serves its Atom
		_, _ = w.Write(fixture(t, "atom_tcmb.xml"))
	}))
	defer srv.Close()

	items, err := NewClient().Fetch(context.Background(), reference.Feed{URL: srv.URL + "/feed", Format: "atom", Language: "en", Timezone: "Europe/Istanbul"})
	if err != nil || len(items) != 3 {
		t.Fatalf("items %d, err %v", len(items), err)
	}
	var se *StatusError
	if _, err := NewClient().Fetch(context.Background(), reference.Feed{URL: srv.URL + "/blocked", Format: "atom"}); !errors.As(err, &se) || se.Code != 403 {
		t.Errorf("err = %v, want a 403 status error", err)
	}
	if _, err := NewClient().Fetch(context.Background(), reference.Feed{URL: srv.URL + "/feed", Format: "atom", Timezone: "Mars/Olympus"}); err == nil {
		t.Error("an unknown timezone must be an error")
	}
}
