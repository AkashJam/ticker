package sweep

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/source"
)

func TestNextFire(t *testing.T) {
	tests := []struct {
		name string
		now  time.Time
		want time.Time // as a UTC instant
	}{
		{"Friday after the fire rolls to Monday", time.Date(2026, 10, 9, 21, 0, 0, 0, time.UTC), time.Date(2026, 10, 12, 20, 15, 0, 0, time.UTC)},
		{"Monday morning fires that afternoon", time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC), time.Date(2026, 10, 12, 20, 15, 0, 0, time.UTC)},
		{"exactly at the fire moves to the next day", time.Date(2026, 10, 12, 20, 15, 0, 0, time.UTC), time.Date(2026, 10, 13, 20, 15, 0, 0, time.UTC)},
		{"Saturday rolls to Monday", time.Date(2026, 10, 10, 15, 0, 0, 0, time.UTC), time.Date(2026, 10, 12, 20, 15, 0, 0, time.UTC)},
		// US clocks go back on Sun 1 Nov 2026: 16:15 becomes 21:15 UTC.
		{"after the autumn change the UTC instant moves", time.Date(2026, 10, 30, 21, 0, 0, 0, time.UTC), time.Date(2026, 11, 2, 21, 15, 0, 0, time.UTC)},
		// US clocks go forward on Sun 14 Mar 2027: 16:15 becomes 20:15 UTC.
		{"after the spring change the UTC instant moves", time.Date(2027, 3, 12, 22, 0, 0, 0, time.UTC), time.Date(2027, 3, 15, 20, 15, 0, 0, time.UTC)},
		// The week the US has changed and Europe has not (25 Oct vs 1 Nov):
		// still 16:15 New York, because the zone is New York, not CET.
		{"between the two clock changes", time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC), time.Date(2026, 10, 27, 20, 15, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NextFire(tt.now, ny)
			if !got.Equal(tt.want) {
				t.Errorf("NextFire = %s, want %s", got.UTC(), tt.want)
			}
			if l := got.In(ny); l.Hour() != 16 || l.Minute() != 15 {
				t.Errorf("local time = %s, want 16:15", l)
			}
		})
	}
}

type fakeLeader struct{ leader bool }

func (f *fakeLeader) IsLeader() bool { return f.leader }

type fakePing struct {
	ok    int
	fails []string
}

func (p *fakePing) Success(context.Context)          { p.ok++ }
func (p *fakePing) Fail(_ context.Context, d string) { p.fails = append(p.fails, d) }

func newScheduler(h *harness, leader bool) (*Scheduler, *fakePing) {
	p := &fakePing{}
	s := NewScheduler(h.r, &fakeLeader{leader: leader}, p, discardLog())
	s.Sleep = func(context.Context, time.Duration) error { return nil }
	return s, p
}

func TestFire_OnlyTheLeaderSweeps(t *testing.T) {
	h := newHarness(source.SweepSymbolCodes(), quoteAt(fridayClose), fridayEvening)
	s, p := newScheduler(h, false)

	s.fire(context.Background())
	if h.q.total() != 0 || p.ok != 0 || len(p.fails) != 0 {
		t.Errorf("a non-leader made %d calls and %d pings", h.q.total(), p.ok+len(p.fails))
	}
}

func TestFire_SuccessAndHolidayBothPingSuccess(t *testing.T) {
	h := newHarness([]string{"RY", "BAP"}, quoteAt(fridayClose), fridayEvening)
	s, p := newScheduler(h, true)
	s.fire(context.Background())
	if p.ok != 1 {
		t.Fatalf("after a full sweep: ok=%d fails=%v", p.ok, p.fails)
	}

	holiday := newHarness([]string{"RY", "BAP"}, quoteAt(time.Date(2026, 10, 8, 20, 0, 0, 0, time.UTC)), fridayEvening)
	s, p = newScheduler(holiday, true)
	s.fire(context.Background())
	if p.ok != 1 || len(p.fails) != 0 {
		t.Errorf("on a holiday: ok=%d fails=%v, want one success", p.ok, p.fails)
	}
}

func TestFire_PartialFailurePingsFailNamingTheSymbols(t *testing.T) {
	h := newHarness([]string{"RY", "KB", "BAP"}, quoteAt(fridayClose), fridayEvening)
	h.q.errs["KB"] = []error{&StatusError{Code: 403}}
	s, p := newScheduler(h, true)

	s.fire(context.Background())
	if p.ok != 0 || len(p.fails) != 1 || !strings.Contains(p.fails[0], "KB") || !strings.Contains(p.fails[0], "2026-10-09") {
		t.Errorf("ok=%d fails=%v", p.ok, p.fails)
	}
}

func TestCatchUp_RunsOnlyWhenLeaderAndABarIsMissing(t *testing.T) {
	saturday := time.Date(2026, 10, 10, 13, 0, 0, 0, time.UTC)

	// Leader, bar missing: sweeps and reports.
	h := newHarness([]string{"RY", "BAP"}, quoteAt(fridayClose), saturday)
	s, p := newScheduler(h, true)
	s.catchUp(context.Background())
	if h.st.upserts != 2 || p.ok != 1 {
		t.Errorf("leader: upserts=%d ok=%d", h.st.upserts, p.ok)
	}

	// Leader, bar present: nothing, and no ping to claim.
	s, p = newScheduler(h, true)
	s.catchUp(context.Background())
	if h.st.upserts != 2 || p.ok != 0 {
		t.Errorf("up to date: upserts=%d ok=%d", h.st.upserts, p.ok)
	}

	// Never the leader: gives up waiting without touching Finnhub.
	h2 := newHarness([]string{"RY", "BAP"}, quoteAt(fridayClose), saturday)
	s, _ = newScheduler(h2, false)
	s.catchUp(context.Background())
	if h2.q.total() != 0 {
		t.Errorf("a non-leader's catch-up made %d calls", h2.q.total())
	}
}
