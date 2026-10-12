package sweep

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/source"
)

func TestDryRunStore_NeverWritesAndReadsThrough(t *testing.T) {
	inner := newFakeStore()
	session := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	inner.bars[key("RY", "1d", session)] = source.Candle{}
	ro := ReadOnly(inner)

	if err := ro.UpsertCandle(context.Background(), source.Candle{Symbol: "BAP", Interval: "1d", Time: session}); err != nil {
		t.Fatal(err)
	}
	if inner.upserts != 0 || len(inner.bars) != 1 {
		t.Errorf("the underlying store was written to: upserts=%d bars=%d", inner.upserts, len(inner.bars))
	}

	if has, err := ro.HasCandle(context.Background(), "RY", "1d", session); err != nil || !has {
		t.Errorf("HasCandle(existing) = %v, %v; want true, nil (it must read through)", has, err)
	}
	if has, _ := ro.HasCandle(context.Background(), "BAP", "1d", session); has {
		t.Error("HasCandle reported a bar that was never written")
	}
}

func TestRun_DryRunValidatesEverySymbolButWritesNothing(t *testing.T) {
	symbols := source.SweepSymbolCodes()
	h := newHarness(symbols, quoteAt(fridayClose), fridayEvening)
	h.r.Store = ReadOnly(h.st)

	res, err := h.r.Run(context.Background(), ModeScheduled)
	if err != nil {
		t.Fatal(err)
	}
	if res.State != StateWritten || res.Written != 28 {
		t.Fatalf("result = %+v, want all 28 validated", res)
	}
	if h.st.upserts != 0 || len(h.st.bars) != 0 {
		t.Errorf("a dry run wrote: upserts=%d bars=%d", h.st.upserts, len(h.st.bars))
	}
	if h.q.total() != 28 {
		t.Errorf("made %d calls, want the same 28 a real run makes", h.q.total())
	}
}

func TestRun_DryRunStillRejectsABadQuote(t *testing.T) {
	h := newHarness([]string{"RY", "BAP"}, quoteAt(fridayClose), fridayEvening)
	h.r.Store = ReadOnly(h.st)
	h.q.quotes["BAP"] = quoteAt(time.Date(2026, 10, 8, 20, 0, 0, 0, time.UTC)) // still Thursday

	res, _ := h.r.Run(context.Background(), ModeScheduled)
	if res.State != StateFailed || len(res.Failed) != 1 || res.Failed[0] != "BAP" {
		t.Errorf("result = %+v; a dry run must report the same failure a real run would", res)
	}
}

func TestRun_DryRunShowsTheHolidayPathOnAWeekend(t *testing.T) {
	saturday := time.Date(2026, 10, 10, 13, 0, 0, 0, time.UTC)
	h := newHarness(source.SweepSymbolCodes(), quoteAt(fridayClose), saturday)
	h.r.Store = ReadOnly(h.st)

	res, err := h.r.Run(context.Background(), ModeScheduled)
	if err != nil {
		t.Fatal(err)
	}
	if res.State != StateHoliday || h.q.total() != 1 || h.st.upserts != 0 {
		t.Errorf("state=%s calls=%d upserts=%d, want holiday after one call and no writes", res.State, h.q.total(), h.st.upserts)
	}
}

func TestParseMode(t *testing.T) {
	good := []struct {
		in   string
		want Mode
	}{
		{"manual", ModeManual}, {"scheduled", ModeScheduled}, {"catch-up", ModeCatchUp},
		{"Scheduled", ModeScheduled}, {" CATCH-UP ", ModeCatchUp},
	}
	for _, g := range good {
		got, err := ParseMode(g.in)
		if err != nil || got != g.want {
			t.Errorf("ParseMode(%q) = %v, %v; want %v", g.in, got, err, g.want)
		}
		if got.String() == "" {
			t.Errorf("Mode %v has no name", got)
		}
	}
	for _, in := range []string{"", "catchup", "dry", "daily"} {
		_, err := ParseMode(in)
		if err == nil || !strings.Contains(err.Error(), "manual, scheduled or catch-up") {
			t.Errorf("ParseMode(%q) error = %v, want one that lists the valid modes", in, err)
		}
	}
	for _, m := range []Mode{ModeManual, ModeScheduled, ModeCatchUp} {
		if back, err := ParseMode(m.String()); err != nil || back != m {
			t.Errorf("round trip of %v failed: %v, %v", m, back, err)
		}
	}
}

func TestExplain(t *testing.T) {
	session := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		res    Result
		dryRun bool
		want   []string
	}{
		{"holiday", Result{State: StateHoliday, Session: session}, true, []string{"No session today", "2026-10-09", "ping success"}},
		{"not final", Result{State: StateNotFinal, Session: session}, true, []string{"still open", "2026-10-09"}},
		{"up to date", Result{State: StateUpToDate, Session: session}, true, []string{"already has its bar", "catch-up would do nothing"}},
		{"dry written", Result{State: StateWritten, Session: session, Written: 28}, true, []string{"28 quotes passed validation", "would write 28 bars"}},
		{"real written", Result{State: StateWritten, Session: session, Written: 28}, false, []string{"Wrote 28 bars", "2026-10-09"}},
		{"dry failed", Result{State: StateFailed, Session: session, Written: 26, Failed: []string{"KB", "BCH"}}, true, []string{"2 symbol(s)", "KB, BCH", "would ping /fail"}},
		{"real failed", Result{State: StateFailed, Session: session, Failed: []string{"KB"}}, false, []string{"1 symbol(s) not written", "KB"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Explain(tt.res, tt.dryRun)
			for _, w := range tt.want {
				if !strings.Contains(got, w) {
					t.Errorf("Explain = %q, missing %q", got, w)
				}
			}
		})
	}
}
