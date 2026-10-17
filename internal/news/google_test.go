package news

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const feed = `<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>x</title>
<item><title>Peso falls - Reuters</title><link>https://news.google.com/rss/articles/AAA</link><pubDate>Fri, 16 Oct 2026 20:05:00 GMT</pubDate><source url="https://www.reuters.com">Reuters</source></item>
<item><title>Peso falls again - Reuters</title><link>https://news.google.com/rss/articles/BBB</link><pubDate>Fri, 16 Oct 2026 21:00:00 GMT</pubDate><source url="https://www.reuters.com">Reuters</source></item>
<item><title>Banks react</title><link>https://news.google.com/rss/articles/CCC</link><pubDate>not a date</pubDate><source url="https://www.ft.com">Financial Times</source></item>
</channel></rss>`

func serve(t *testing.T, status int, body string, seen *string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if seen != nil {
			*seen = r.URL.RawQuery
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return &Client{http: srv.Client(), baseURL: srv.URL}
}

func TestSearchParsesItemsAndOutlets(t *testing.T) {
	var q string
	c := serve(t, http.StatusOK, feed, &q)
	items, err := c.Search(context.Background(), `Mexico (economy OR "Mexican peso")`, 2*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 || DistinctOutlets(items) != 2 {
		t.Fatalf("items %d, outlets %d", len(items), DistinctOutlets(items))
	}
	if items[0].Outlet != "Reuters" || items[0].OutletURL != "https://www.reuters.com" || !items[0].Published.Equal(time.Date(2026, 10, 16, 20, 5, 0, 0, time.UTC)) {
		t.Errorf("first item = %+v", items[0])
	}
	if !items[2].Published.IsZero() {
		t.Errorf("an unparsable date must be zero, got %v", items[2].Published)
	}
	for _, want := range []string{"when%3A2d", "hl=en-US", "ceid=US%3Aen", "Mexican+peso"} {
		if !strings.Contains(q, want) {
			t.Errorf("query %q lacks %q", q, want)
		}
	}
}

func TestWhenWindow(t *testing.T) {
	for d, want := range map[time.Duration]string{
		0: "", 24 * time.Hour: "1d", 48 * time.Hour: "2d", 6 * time.Hour: "6h", 30 * time.Minute: "1h",
	} {
		if got := whenWindow(d); got != want {
			t.Errorf("whenWindow(%v) = %q, want %q", d, got, want)
		}
	}
}

func TestSearchErrors(t *testing.T) {
	var se *StatusError
	if _, err := serve(t, http.StatusTooManyRequests, "", nil).Search(context.Background(), "x", 0); !errors.As(err, &se) || se.Code != 429 {
		t.Errorf("err = %v", err)
	}
	if _, err := serve(t, http.StatusOK, "<html>", nil).Search(context.Background(), "x", 0); err == nil {
		t.Error("a non-feed body must be an error")
	}
	if _, err := serve(t, http.StatusOK, feed, nil).Search(context.Background(), "  ", 0); err == nil {
		t.Error("an empty query must be refused before any request")
	}
}
