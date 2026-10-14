package backfill

import (
	"strings"
	"testing"
	"time"
)

func ny(t *testing.T) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

func f(v float64) *float64 { return &v }

func julUTC(d, h int) int64 {
	return time.Date(2026, time.July, d, h, 30, 0, 0, time.UTC).Unix()
}

func TestParseChart(t *testing.T) {
	const body = `{"chart":{"result":[{"timestamp":[1782912600,1783017000],
	"indicators":{"quote":[{"open":[10.1,null],"high":[11,null],"low":[9,null],"close":[10.5,null]}],
	"adjclose":[{"adjclose":[10.4,null]}]},
	"events":{"dividends":{"1783017000":{"amount":0.1,"date":1783017000}},"splits":{}}}],"error":null}}`
	c, err := ParseChart("RY", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Timestamp) != 2 || c.Close[1] != nil || *c.Close[0] != 10.5 {
		t.Errorf("parsed %+v", c)
	}
	if len(c.Dividends) != 1 || len(c.Splits) != 0 {
		t.Errorf("events: %v %v", c.Dividends, c.Splits)
	}
}

func TestParseChartRejectsMisalignedArrays(t *testing.T) {
	const body = `{"chart":{"result":[{"timestamp":[1,2],"indicators":{"quote":[{"open":[1],"high":[1,2],"low":[1,2],"close":[1,2]}],"adjclose":[{"adjclose":[1,2]}]}}]}}`
	if _, err := ParseChart("RY", strings.NewReader(body)); err == nil {
		t.Error("misaligned open array accepted")
	}
}

func TestBarsSkipsNullsRoundsAndDatesInNewYork(t *testing.T) {
	c := Chart{
		Symbol: "RY",
		// 02:30 UTC on 2 Jul is 22:30 on 1 Jul in New York: the session date is 1 Jul.
		Timestamp: []int64{julUTC(2, 2), julUTC(6, 13), julUTC(7, 13)},
		Open:      []*float64{f(100.123456), nil, f(1)},
		High:      []*float64{f(101.99999908), f(2), f(2)},
		Low:       []*float64{f(99), f(1), f(1)},
		Close:     []*float64{f(100.5), f(1), f(1.5)},
	}
	bars, err := Bars(c, ny(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(bars) != 2 {
		t.Fatalf("got %d bars, want 2 (the null-open day is skipped, not filled)", len(bars))
	}
	if got := bars[0].Session.Format(time.DateOnly); got != "2026-07-01" {
		t.Errorf("session = %s, want 2026-07-01", got)
	}
	if bars[0].Open != 100.1235 || bars[0].High != 102 {
		t.Errorf("rounding: open %v high %v", bars[0].Open, bars[0].High)
	}
	if got := bars[1].Session.Format(time.DateOnly); got != "2026-07-07" {
		t.Errorf("second session = %s", got)
	}
}

func TestBarsRejectsWeekendAndDuplicateDates(t *testing.T) {
	loc := ny(t)
	base := Chart{Symbol: "RY", Open: []*float64{f(1), f(1)}, High: []*float64{f(1), f(1)}, Low: []*float64{f(1), f(1)}, Close: []*float64{f(1), f(1)}}

	weekend := base
	weekend.Timestamp = []int64{julUTC(4, 13), julUTC(6, 13)} // a Saturday
	if _, err := Bars(weekend, loc); err == nil {
		t.Error("Saturday bar accepted")
	}
	dup := base
	dup.Timestamp = []int64{julUTC(6, 13), julUTC(6, 14)}
	if _, err := Bars(dup, loc); err == nil {
		t.Error("two bars on one date accepted")
	}
}

func adjChart(adj ...float64) Chart {
	c := Chart{Symbol: "RY"}
	for i, a := range adj {
		c.Timestamp = append(c.Timestamp, julUTC(6+i, 13))
		c.Close = append(c.Close, f(100))
		c.AdjClose = append(c.AdjClose, f(a))
	}
	return c
}

func TestCheckCorporateActions(t *testing.T) {
	loc := ny(t)

	if err := CheckCorporateActions(adjChart(99.5, 99.5, 99.5), loc); err != nil {
		t.Errorf("flat ratio: %v", err)
	}

	split := adjChart(99.5, 99.5)
	split.Splits = []int64{julUTC(7, 13)}
	if err := CheckCorporateActions(split, loc); err == nil {
		t.Error("split accepted")
	}

	step := adjChart(99.0, 99.0, 99.5) // ratio steps on the third day
	if err := CheckCorporateActions(step, loc); err == nil {
		t.Error("unexplained adjclose step accepted")
	}
	step.Dividends = []int64{julUTC(8, 13)}
	if err := CheckCorporateActions(step, loc); err != nil {
		t.Errorf("step on an ex-date: %v", err)
	}
}

func TestBeforeAndCount(t *testing.T) {
	bars := []Bar{
		{Symbol: "RY", Session: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)},
		{Symbol: "RY", Session: time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)},
	}
	got := Before(bars, Cutoff)
	if len(got) != 1 || got[0].Session.Day() != 8 {
		t.Errorf("Before kept %v", got)
	}
	if len(bars) != 2 {
		t.Error("Before modified its input")
	}
	if err := CheckCount("RY", got); err == nil {
		t.Error("one close passed the 61-close floor")
	}
}

func TestRenderUp(t *testing.T) {
	bars := []Bar{
		{Symbol: "RY", Session: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), Open: 100.1235, High: 102, Low: 99, Close: 100.5},
		{Symbol: "AAPL", Session: time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC), Open: 1, High: 2, Low: 1, Close: 1.5},
	}
	sql := RenderUp(bars, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC))
	for _, want := range []string{
		"('2026-07-01 00:00:00+00', 'RY', '1d', 100.1235, 102, 99, 100.5, NULL),\n",
		"('2026-07-02 00:00:00+00', 'AAPL', '1d', 1, 2, 1, 1.5, NULL)\n",
		"ON CONFLICT (symbol, interval, time) DO NOTHING;",
		"2026-07-01 to 2026-10-08", "fetched 2026-10-14", "never adjclose",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing %q in:\n%s", want, sql)
		}
	}
}

func TestRenderDownIsBoundedOnBothSides(t *testing.T) {
	sql := RenderDown([]string{"RY", "AAPL"})
	for _, want := range []string{"interval = '1d'", "time < '2026-10-09 00:00:00+00'", "symbol IN ('RY', 'AAPL')"} {
		if !strings.Contains(sql, want) {
			t.Errorf("down migration lacks %q:\n%s", want, sql)
		}
	}
}

func TestCompare(t *testing.T) {
	day := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	yahoo := []Bar{
		{Symbol: "RY", Session: day, Open: 100, High: 101, Low: 99, Close: 100.0035},
		{Symbol: "AAPL", Session: day, Open: 1, High: 1, Low: 1, Close: 1},
	}
	stored := map[string]Bar{key("RY", day): {Symbol: "RY", Session: day, Open: 100, High: 101, Low: 99, Close: 100}}
	diffs, compared := Compare(yahoo, stored, 0.5)
	if compared != 1 {
		t.Errorf("compared %d, want 1 (AAPL has no stored bar)", compared)
	}
	if len(diffs) != 0 {
		t.Errorf("0.35 bp difference flagged at a 0.5 bp tolerance: %v", diffs)
	}
	diffs, _ = Compare(yahoo, stored, 0.1)
	if len(diffs) != 1 || diffs[0].Field != "close" {
		t.Errorf("diffs = %v", diffs)
	}
}
