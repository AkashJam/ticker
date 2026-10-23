package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/source"
)

var (
	tickAt = time.Date(2026, 10, 14, 15, 30, 0, 0, time.UTC)
	barAt  = time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
)

func TestBuildSnapshot_AsOf(t *testing.T) {
	prev := &source.Candle{Close: 100}
	today := &source.Candle{Time: barAt, High: 112, Low: 99, Close: 110}

	t.Run("cached tick dates the price", func(t *testing.T) {
		got := buildSnapshot("RY", &source.NormalizedTick{Price: 105, Timestamp: tickAt}, today, prev)
		if got.AsOf == nil || !got.AsOf.Equal(tickAt) || got.Price != 105 || got.Change != 5 {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("no tick falls back to the latest daily bar", func(t *testing.T) {
		got := buildSnapshot("RY", nil, today, prev)
		if got.AsOf == nil || !got.AsOf.Equal(barAt) || got.Price != 110 {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("nothing at all is a null vintage, not a made-up one", func(t *testing.T) {
		got := buildSnapshot("RY", nil, nil, nil)
		if got.AsOf != nil {
			t.Errorf("asOf = %v, want nil", got.AsOf)
		}
	})
}

// The snapshot is the public contract: it carries a vintage and says nothing
// about whether the source was real (ADR-006).
func TestSnapshotJSONHasAsOfAndNoSimulatedFlag(t *testing.T) {
	for name, snap := range map[string]snapshotResponse{
		"vintage": buildSnapshot("SIM:NOVA", &source.NormalizedTick{Price: 1, Timestamp: tickAt, Simulated: true}, nil, nil),
		"null":    buildSnapshot("RY", nil, nil, nil),
	} {
		b, err := json.Marshal(snap)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		if _, ok := m["asOf"]; !ok {
			t.Errorf("%s: no asOf key in %s", name, b)
		}
		for _, gone := range []string{"simulated", "updatedAt"} {
			if _, ok := m[gone]; ok {
				t.Errorf("%s: %q still in %s", name, gone, b)
			}
		}
	}
}

// M31 (phase18.md step 1): the daily bar is written by the 16:15 sweep, so
// until then the newest one is the previous close. AMX on 22 Oct 2026 read
// +9.00% (against 20 Oct's 22.90) when it had risen 2.9% from 21 Oct's 24.29.
func TestPickDay_AcrossTheSession(t *testing.T) {
	ny := newYork
	at := func(d, h, m int) time.Time { return time.Date(2026, 10, d, h, m, 0, 0, ny) }
	bar := func(d int, close float64) source.Candle {
		return source.Candle{Time: time.Date(2026, 10, d, 0, 0, 0, 0, time.UTC), High: close + 1, Low: close - 1, Close: close}
	}
	daily := []source.Candle{bar(20, 22.90), bar(21, 24.29)}
	tick := func(ts time.Time) *source.NormalizedTick {
		return &source.NormalizedTick{Price: 25, Timestamp: ts}
	}

	cases := []struct {
		name      string
		daily     []source.Candle
		cached    *source.NormalizedTick
		wantToday bool // a daily bar for today exists
		wantPrev  float64
	}{
		{"before the open, on the day's pre-market trade", daily, tick(at(22, 8, 0)), false, 24.29},
		{"mid-session", daily, tick(at(22, 10, 0)), false, 24.29},
		{"after the close, before the sweep", daily, tick(at(22, 16, 10)), false, 24.29},
		{"after-hours, before the sweep", daily, tick(at(22, 18, 30)), false, 24.29},
		{"after the sweep, today's bar is the newest", []source.Candle{bar(21, 24.29), bar(22, 25)}, tick(at(22, 16, 30)), true, 24.29},
		{"the next morning, today's bar is yesterday's", []source.Candle{bar(21, 24.29), bar(22, 25)}, tick(at(23, 9, 45)), false, 25},
		{"a trade after UTC midnight still belongs to the New York date", daily, tick(time.Date(2026, 10, 22, 3, 30, 0, 0, time.UTC)), true, 22.90},
		{"no cached trade keeps the newest bar as today", daily, nil, true, 22.90},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			today, prev := pickDay(c.daily, c.cached)
			if (today != nil) != c.wantToday {
				t.Errorf("today = %v, want present %v", today, c.wantToday)
			}
			if prev == nil || prev.Close != c.wantPrev {
				t.Errorf("prev = %v, want close %v", prev, c.wantPrev)
			}
		})
	}

	t.Run("one bar only", func(t *testing.T) {
		today, prev := pickDay(daily[:1], tick(at(22, 10, 0)))
		if today != nil || prev == nil || prev.Close != 22.90 {
			t.Errorf("today=%v prev=%v", today, prev)
		}
		today, prev = pickDay(daily[:1], nil)
		if today == nil || prev != nil {
			t.Errorf("no tick: today=%v prev=%v", today, prev)
		}
	})
	t.Run("no bars", func(t *testing.T) {
		if today, prev := pickDay(nil, tick(at(22, 10, 0))); today != nil || prev != nil {
			t.Errorf("today=%v prev=%v", today, prev)
		}
	})
}

func TestBuildSnapshot_MidSessionChangeIsAgainstThePreviousClose(t *testing.T) {
	prev := &source.Candle{Close: 24.29}
	live := liveDay([]source.Candle{
		{High: 24.80, Low: 24.10},
		{High: 25.10, Low: 24.60},
	})
	got := buildSnapshot("AMX", &source.NormalizedTick{Price: 25, Timestamp: tickAt}, live, prev)
	if got.PrevClose != 24.29 || got.Change != 0.71 || got.ChangePercent != 2.92 {
		t.Errorf("change = %v (%v%%) against %v, want 0.71 (2.92%%) against 24.29", got.Change, got.ChangePercent, got.PrevClose)
	}
	if got.DayHigh != 25.10 || got.DayLow != 24.10 {
		t.Errorf("range = %v–%v, want 24.10–25.10", got.DayLow, got.DayHigh)
	}

	t.Run("the live price widens a range the bars have not caught up with", func(t *testing.T) {
		got := buildSnapshot("AMX", &source.NormalizedTick{Price: 26, Timestamp: tickAt}, live, prev)
		if got.DayHigh != 26 {
			t.Errorf("dayHigh = %v, want 26", got.DayHigh)
		}
	})
	t.Run("no live bars yet leaves the range at the price", func(t *testing.T) {
		got := buildSnapshot("AMX", &source.NormalizedTick{Price: 25, Timestamp: tickAt}, liveDay(nil), prev)
		if got.DayHigh != 25 || got.DayLow != 25 || got.Change != 0.71 {
			t.Errorf("got %+v", got)
		}
	})
}
