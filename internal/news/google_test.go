package news

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
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
	items, err := c.Search(context.Background(), `Mexico (economy OR "Mexican peso")`, time.Date(2026, 10, 14, 15, 0, 0, 0, time.UTC), time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC))
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
	for _, want := range []string{"after%3A2026-10-14", "before%3A2026-10-18", "hl=en-US", "ceid=US%3Aen", "Mexican+peso"} {
		if !strings.Contains(q, want) {
			t.Errorf("query %q lacks %q", q, want)
		}
	}
}

func TestSearchErrors(t *testing.T) {
	var se *StatusError
	if _, err := serve(t, http.StatusTooManyRequests, "", nil).Search(context.Background(), "x", time.Time{}, time.Time{}); !errors.As(err, &se) || se.Code != 429 {
		t.Errorf("err = %v", err)
	}
	if _, err := serve(t, http.StatusOK, "<html>", nil).Search(context.Background(), "x", time.Time{}, time.Time{}); err == nil {
		t.Error("a non-feed body must be an error")
	}
	if _, err := serve(t, http.StatusOK, feed, nil).Search(context.Background(), "  ", time.Time{}, time.Time{}); err == nil {
		t.Error("an empty query must be refused before any request")
	}
}

// --- grouping ---

var close16 = time.Date(2026, 10, 15, 20, 0, 0, 0, time.UTC)

func at(h int) time.Time { return time.Date(2026, 10, 15, h, 0, 0, 0, time.UTC) }

func TestGroupKeepsTheTwoEarliestPerOutletAndOrdersOutletsEarliestFirst(t *testing.T) {
	items := []Item{
		{Title: "c3", Outlet: "Reuters", OutletURL: "https://www.reuters.com", Published: at(22)},
		{Title: "c1", Outlet: "Reuters", OutletURL: "https://www.reuters.com", Published: at(9)},
		{Title: "c2", Outlet: "Reuters", OutletURL: "https://www.reuters.com", Published: at(15)},
		{Title: "f1", Outlet: "FT", OutletURL: "https://www.ft.com", Published: at(7)},
		{Title: "b1", Outlet: "Bloomberg", OutletURL: "https://www.bloomberg.com", Published: at(21)},
	}
	got := Group(items, close16, ExcludedList{})
	names := []string{}
	for _, o := range got.Outlets {
		names = append(names, o.Name)
	}
	if !slices.Equal(names, []string{"FT", "Reuters", "Bloomberg"}) {
		t.Fatalf("outlets = %v, want earliest first", names)
	}
	r := got.Outlets[1]
	if len(r.Items) != 2 || r.Items[0].Title != "c1" || r.Items[1].Title != "c2" {
		t.Errorf("Reuters kept %+v, want its two earliest", r.Items)
	}
	if got.Outlets[1].Items[1].Side != SideBefore || got.Outlets[2].Items[0].Side != SideAfter {
		t.Error("an item is before the close or, at it or later, after")
	}
	if got.Total != 5 {
		t.Errorf("total = %d, want 5 (the query's, before the cap)", got.Total)
	}
}

func TestGroupSideAtTheCloseIsAfter(t *testing.T) {
	got := Group([]Item{{Title: "x", Outlet: "A", Published: close16}}, close16, ExcludedList{})
	if got.Outlets[0].Items[0].Side != SideAfter {
		t.Error("the close itself is after")
	}
}

func TestGroupDropsMachineOutletsByHostThenName(t *testing.T) {
	list := ExcludedList{Excluded: []Entry{
		{Name: "MarketBeat", Hosts: []string{"marketbeat.com"}},
		{Name: "Stock Titan", Hosts: []string{"stocktitan.net"}},
	}}
	items := []Item{
		{Title: "a", Outlet: "MarketBeat", OutletURL: "https://www.marketbeat.com/x", Published: at(10)},
		{Title: "b", Outlet: "Some renamed outlet", OutletURL: "https://news.marketbeat.com", Published: at(10)}, // a subdomain
		{Title: "c", Outlet: "stock titan", Published: at(10)},                                                   // no url: by name
		{Title: "d", Outlet: "Reuters", OutletURL: "https://www.reuters.com", Published: at(10)},
		{Title: "e", Outlet: "Notmarketbeat", OutletURL: "https://notmarketbeat.com", Published: at(10)}, // not a subdomain
	}
	got := Group(items, close16, list)
	if got.Excluded != 3 || len(got.Outlets) != 2 {
		t.Fatalf("excluded %d, outlets %+v", got.Excluded, got.Outlets)
	}
}

func TestGroupDropsNonLatinTitlesAndUndatedItems(t *testing.T) {
	items := []Item{
		{Title: "Банк России сохранил ставку", Outlet: "RU", Published: at(10)},
		{Title: "日本銀行 policy", Outlet: "JP", Published: at(10)}, // 2 Latin of 6 letters... mostly CJK
		{Title: "Peso steadies after Banxico", Outlet: "Reuters", Published: at(10)},
		{Title: "No date", Outlet: "Reuters", Published: time.Time{}},
		{Title: "Bank of Mexico keeps rate – 12%", Outlet: "AP", Published: at(11)}, // digits and punctuation are not letters
	}
	got := Group(items, close16, ExcludedList{})
	if got.NonEnglish != 2 || got.Undated != 1 || len(got.Outlets) != 2 {
		t.Errorf("non-English %d, undated %d, outlets %+v", got.NonEnglish, got.Undated, got.Outlets)
	}
}

func TestGroupEmptyAndUnnamedOutlets(t *testing.T) {
	got := Group(nil, close16, ExcludedList{})
	if got.Outlets == nil || len(got.Outlets) != 0 || got.Total != 0 {
		t.Errorf("an empty result must be an empty list: %+v", got)
	}
	got = Group([]Item{{Title: "x", Outlet: "", OutletURL: "https://www.example.org/a", Published: at(9)}, {Title: "y", Published: at(9)}}, close16, ExcludedList{})
	if len(got.Outlets) != 2 {
		t.Fatalf("outlets = %+v", got.Outlets)
	}
	for _, o := range got.Outlets {
		if o.Name == "" {
			t.Error("an outlet must always have a name")
		}
	}
}

func TestEmbeddedListIsTheElevenOutletsOfBearing111(t *testing.T) {
	b, err := os.ReadFile("../reference/embedded/outlets.json")
	if err != nil {
		t.Fatal(err)
	}
	var l ExcludedList
	if err := json.Unmarshal(b, &l); err != nil {
		t.Fatal(err)
	}
	if len(l.Excluded) != 12 || l.Source == "" || l.AsOf == "" {
		t.Fatalf("%d entries (#111 names eleven outlets, one of which is two entries: Kalkine Media and kalkine.ca)", len(l.Excluded))
	}
	for _, name := range []string{"MarketBeat", "ad-hoc-news.de", "Kalkine Media", "kalkine.ca", "Bybit", "Traders Union", "BigGo Finance", "tmgm.com", "CryptoRank", "Moomoo", "TipRanks", "Stock Titan"} {
		found := false
		for _, e := range l.Excluded {
			found = found || e.Name == name
		}
		if !found {
			t.Errorf("%s is missing", name)
		}
	}
	for _, e := range l.Excluded {
		if len(e.Hosts) == 0 {
			t.Errorf("%s has no host to match", e.Name)
		}
	}
}
