package dividend

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var ny = mustLoc("America/New_York")

func mustLoc(name string) *time.Location {
	l, err := time.LoadLocation(name)
	if err != nil {
		panic(err)
	}
	return l
}

// exTimestamp is how Yahoo stamps an ex-date: the open of that day in New York.
func exTimestamp(d int) int64 {
	return time.Date(2026, time.July, d, 9, 30, 0, 0, ny).Unix()
}

func body(divs map[string][2]any) string {
	var parts []string
	for k, v := range divs {
		parts = append(parts, fmt.Sprintf(`"%s":{"amount":%v,"date":%v}`, k, v[0], v[1]))
	}
	events := ""
	if len(parts) > 0 {
		events = fmt.Sprintf(`,"events":{"dividends":{%s}}`, strings.Join(parts, ","))
	}
	return fmt.Sprintf(`{"chart":{"result":[{"meta":{"currency":"USD"}%s}],"error":null}}`, events)
}

func TestParseReadsAmountDateAndCurrency(t *testing.T) {
	ts := exTimestamp(27)
	c, err := Parse(strings.NewReader(body(map[string][2]any{fmt.Sprint(ts): {1.249, ts}})))
	if err != nil {
		t.Fatal(err)
	}
	if c.Currency != "USD" || len(c.Dividends) != 1 || c.Dividends[0].Amount != 1.249 {
		t.Fatalf("chart = %+v", c)
	}
}

func TestParseNoEventsIsNoDividendsNotAnError(t *testing.T) {
	c, err := Parse(strings.NewReader(body(nil)))
	if err != nil || len(c.Dividends) != 0 {
		t.Fatalf("chart = %+v, err = %v", c, err)
	}
}

func TestParseRejectsYahooErrorsAndOddShapes(t *testing.T) {
	for name, in := range map[string]string{
		"yahoo error": `{"chart":{"result":null,"error":{"code":"Not Found","description":"No data found"}}}`,
		"no result":   `{"chart":{"result":[],"error":null}}`,
		"not json":    `<html>Too Many Requests</html>`,
	} {
		if _, err := Parse(strings.NewReader(in)); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestOnSessionMatchesTheNewYorkDay(t *testing.T) {
	ts := time.Unix(exTimestamp(27), 0)
	c := Chart{Currency: "USD", Dividends: []Entry{{Date: ts, Amount: 1.249}}}

	got := OnSession(c, time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC), ny)
	if got == nil || got.Date != "2026-07-27" || got.Amount != 1.249 || got.Currency != "USD" {
		t.Fatalf("on the ex-date: %+v", got)
	}
	// The stamp is 13:30 UTC, still the 27th in New York; the days either side are not.
	for _, d := range []int{24, 26, 28} {
		if got := OnSession(c, time.Date(2026, 7, d, 0, 0, 0, 0, time.UTC), ny); got != nil {
			t.Errorf("July %d: want none, got %+v", d, got)
		}
	}
}

func TestOnSessionUsesTheNewYorkDayNotTheUTCOne(t *testing.T) {
	// 23:00 on the 27th in New York is already the 28th in UTC.
	late := time.Date(2026, 7, 27, 23, 0, 0, 0, ny)
	c := Chart{Dividends: []Entry{{Date: late, Amount: 0.5}}}
	if OnSession(c, time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC), ny) == nil {
		t.Error("the ex-date is the 27th in New York")
	}
	if OnSession(c, time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC), ny) != nil {
		t.Error("the 28th is the UTC date, not the New York one")
	}
}

func clientFor(h http.HandlerFunc) (*Client, func()) {
	srv := httptest.NewServer(h)
	return &Client{base: srv.URL + "/", http: srv.Client(), loc: ny}, srv.Close
}

func TestExDividendFindsTheOneOnTheSession(t *testing.T) {
	ts := exTimestamp(27)
	var gotUA, gotPath, gotEvents string
	c, done := clientFor(func(w http.ResponseWriter, r *http.Request) {
		gotUA, gotPath, gotEvents = r.UserAgent(), r.URL.Path, r.URL.Query().Get("events")
		_, _ = fmt.Fprint(w, body(map[string][2]any{fmt.Sprint(ts): {1.249, ts}}))
	})
	defer done()

	d, err := c.ExDividend(context.Background(), "RY", time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC))
	if err != nil || d == nil || d.Amount != 1.249 || d.Date != "2026-07-27" {
		t.Fatalf("dividend = %+v, err = %v", d, err)
	}
	if gotPath != "/RY" || gotEvents != "div" || !strings.HasPrefix(gotUA, "Mozilla/") {
		t.Errorf("request: path %q events %q user agent %q", gotPath, gotEvents, gotUA)
	}
}

func TestExDividendNoneOnAnOrdinarySessionIsNilNotAnError(t *testing.T) {
	c, done := clientFor(func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, body(nil)) })
	defer done()
	d, err := c.ExDividend(context.Background(), "RY", time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC))
	if err != nil || d != nil {
		t.Fatalf("dividend = %+v, err = %v", d, err)
	}
}

func TestExDividendAnOtherDayInTheWindowIsNotTheSession(t *testing.T) {
	// The window runs from the day before to two days after, so a neighbour's
	// ex-date comes back and must not count.
	ts := exTimestamp(28)
	c, done := clientFor(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, body(map[string][2]any{fmt.Sprint(ts): {1.0, ts}}))
	})
	defer done()
	d, err := c.ExDividend(context.Background(), "RY", time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC))
	if err != nil || d != nil {
		t.Fatalf("dividend = %+v, err = %v", d, err)
	}
}

func TestExDividendA429IsAnError(t *testing.T) {
	c, done := clientFor(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTooManyRequests) })
	defer done()
	d, err := c.ExDividend(context.Background(), "RY", time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC))
	if err == nil || d != nil || !strings.Contains(err.Error(), "429") {
		t.Fatalf("dividend = %+v, err = %v", d, err)
	}
}

func TestExDividendAnHTMLBodyIsAnError(t *testing.T) {
	c, done := clientFor(func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, "<html>blocked</html>") })
	defer done()
	if _, err := c.ExDividend(context.Background(), "RY", time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("want an error")
	}
}
