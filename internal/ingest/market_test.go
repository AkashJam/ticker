package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func nyLoc(t *testing.T) *time.Location {
	t.Helper()
	l, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// fakeStatus returns a scripted answer and counts calls.
type fakeStatus struct {
	state MarketState
	err   error
	calls int
}

func (f *fakeStatus) fn(context.Context) (MarketState, error) {
	f.calls++
	return f.state, f.err
}

func newGate(t *testing.T, f *fakeStatus) *MarketGate {
	t.Helper()
	return NewMarketGate(f.fn, nyLoc(t), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// at builds a 2026 New York wall-clock time.
func at(t *testing.T, m time.Month, d, h, min int) time.Time {
	t.Helper()
	return time.Date(2026, m, d, h, min, 0, 0, nyLoc(t))
}

func TestGate_ClosedAnswerIsCachedToNextWeekdayOpen(t *testing.T) {
	ctx := context.Background()
	f := &fakeStatus{state: MarketState{Open: false}}
	g := newGate(t, f)

	// Friday 16 Oct 2026, 21:00: closed until Monday 19 Oct 09:30.
	fri := at(t, time.October, 16, 21, 0)
	if !g.Closed(ctx, fri) {
		t.Fatal("closed answer was not reported closed")
	}
	if f.calls != 1 {
		t.Fatalf("calls = %d, want 1", f.calls)
	}
	for _, now := range []time.Time{fri.Add(time.Hour), at(t, time.October, 17, 12, 0), at(t, time.October, 18, 23, 0), at(t, time.October, 19, 9, 29)} {
		if !g.Closed(ctx, now) {
			t.Errorf("not closed at %s", now)
		}
	}
	if f.calls != 1 {
		t.Errorf("calls = %d after the whole weekend, want still 1", f.calls)
	}
}

func TestGate_AsksAgainAtResumeAndCachesAHolidayToTheNextWeekday(t *testing.T) {
	ctx := context.Background()
	f := &fakeStatus{state: MarketState{Open: false, Holiday: "Thanksgiving Day"}}
	g := newGate(t, f)

	// Wed 25 Nov evening -> cached to Thu 26 Nov 09:30 (a weekday, though a holiday).
	g.Closed(ctx, at(t, time.November, 25, 20, 0))
	if f.calls != 1 {
		t.Fatalf("calls = %d", f.calls)
	}
	// At the resume time the cache expires, and Finnhub is asked again.
	thu := at(t, time.November, 26, 10, 0)
	if !g.Closed(ctx, thu) {
		t.Fatal("holiday not reported closed")
	}
	if f.calls != 2 {
		t.Fatalf("calls = %d, want a second ask once the window ended", f.calls)
	}
	// ...and that answer holds to Friday 27 Nov 09:30, not through it.
	if !g.Closed(ctx, at(t, time.November, 27, 9, 0)) || f.calls != 2 {
		t.Errorf("holiday answer did not hold to the next weekday's open (calls=%d)", f.calls)
	}
}

func TestGate_OpenAnswerIsRecheckedOnlyAfterTheInterval(t *testing.T) {
	ctx := context.Background()
	f := &fakeStatus{state: MarketState{Open: true, Session: "regular"}}
	g := newGate(t, f)
	now := at(t, time.October, 14, 11, 0)

	if g.Closed(ctx, now) {
		t.Fatal("an open market reported closed")
	}
	g.Closed(ctx, now.Add(time.Minute))
	g.Closed(ctx, now.Add(4*time.Minute))
	if f.calls != 1 {
		t.Fatalf("calls = %d inside the recheck interval, want 1", f.calls)
	}
	g.Closed(ctx, now.Add(openRecheck))
	if f.calls != 2 {
		t.Errorf("calls = %d, want a recheck after %s", f.calls, openRecheck)
	}
}

func TestGate_ErrorFailsTowardAlertingAndRetriesNoSoonerThanErrorRetry(t *testing.T) {
	ctx := context.Background()
	f := &fakeStatus{err: errors.New("finnhub down")}
	g := newGate(t, f)
	now := at(t, time.October, 14, 22, 0)

	if g.Closed(ctx, now) {
		t.Fatal("a failed lookup must not read as closed")
	}
	g.Closed(ctx, now.Add(10*time.Second))
	if f.calls != 1 {
		t.Fatalf("calls = %d, want no retry inside %s", f.calls, errorRetry)
	}
	f.err, f.state = nil, MarketState{Open: false}
	if !g.Closed(ctx, now.Add(errorRetry)) {
		t.Errorf("did not recover once the call succeeded")
	}
}

func TestGate_NextOpenKeepsNineThirtyLocalAcrossTheClockChange(t *testing.T) {
	g := newGate(t, &fakeStatus{})
	// US clocks go back on Sunday 1 Nov 2026. Friday 30 Oct evening -> Monday 2 Nov 09:30 EST.
	got := g.nextOpen(at(t, time.October, 30, 21, 0))
	want := at(t, time.November, 2, 9, 30)
	if !got.Equal(want) {
		t.Errorf("nextOpen = %s, want %s", got, want)
	}
	if got.UTC().Hour() != 14 { // 09:30 EST is 14:30 UTC; EDT would be 13:30
		t.Errorf("UTC hour = %d, want 14 (EST)", got.UTC().Hour())
	}
	// Before the open on a weekday: the same day.
	if got := g.nextOpen(at(t, time.October, 14, 8, 0)); !got.Equal(at(t, time.October, 14, 9, 30)) {
		t.Errorf("pre-market nextOpen = %s, want the same day 09:30", got)
	}
}

func TestGate_ResumeFloorAppearsOnlyAfterTheWindowEnds(t *testing.T) {
	ctx := context.Background()
	g := newGate(t, &fakeStatus{state: MarketState{Open: false}})
	g.Closed(ctx, at(t, time.October, 14, 20, 0))

	if f := g.ResumeFloor(at(t, time.October, 15, 9, 0)); !f.IsZero() {
		t.Errorf("floor = %s before the window ended, want zero", f)
	}
	open := at(t, time.October, 15, 9, 30)
	if f := g.ResumeFloor(open.Add(time.Minute)); !f.Equal(open) {
		t.Errorf("floor = %s, want %s", f, open)
	}
}

const maxAge = 30 * time.Minute

func TestTickFresh_NilGateIsPlainTickAge(t *testing.T) {
	now := at(t, time.October, 14, 12, 0)
	if !TickFresh(context.Background(), now, now.Add(-29*time.Minute), maxAge, nil) {
		t.Error("a 29-minute-old tick must be fresh")
	}
	if TickFresh(context.Background(), now, now.Add(-31*time.Minute), maxAge, nil) {
		t.Error("a 31-minute-old tick must be stale with no gate (the sim never goes quiet)")
	}
}

func TestTickFresh_FreshTickMakesNoStatusCall(t *testing.T) {
	f := &fakeStatus{state: MarketState{Open: true}}
	g := newGate(t, f)
	now := at(t, time.October, 14, 12, 0)
	if !TickFresh(context.Background(), now, now.Add(-time.Second), maxAge, g) {
		t.Fatal("fresh tick reported stale")
	}
	if f.calls != 0 {
		t.Errorf("status calls = %d on a trading day with ticks flowing, want 0", f.calls)
	}
}

func TestTickFresh_StaleWhileClosedIsHealthy_StaleWhileOpenIsNot(t *testing.T) {
	ctx := context.Background()
	night := at(t, time.October, 14, 23, 0)
	last := at(t, time.October, 14, 16, 0)

	closed := newGate(t, &fakeStatus{state: MarketState{Open: false}})
	if !TickFresh(ctx, night, last, maxAge, closed) {
		t.Error("seven hours of silence overnight must be healthy")
	}
	open := newGate(t, &fakeStatus{state: MarketState{Open: true}})
	if TickFresh(ctx, at(t, time.October, 14, 11, 0), at(t, time.October, 14, 10, 0), maxAge, open) {
		t.Error("an hour of silence mid-session must be unhealthy")
	}
	broken := newGate(t, &fakeStatus{err: errors.New("down")})
	if TickFresh(ctx, night, last, maxAge, broken) {
		t.Error("a failed status lookup must not hide stale ticks")
	}
}

func TestTickFresh_FirstCheckAfterTheOpenDoesNotCountTheNight(t *testing.T) {
	ctx := context.Background()
	f := &fakeStatus{state: MarketState{Open: false}}
	g := newGate(t, f)
	last := at(t, time.October, 14, 16, 0)

	// Overnight: closed, cached to 09:30.
	if !TickFresh(ctx, at(t, time.October, 14, 22, 0), last, maxAge, g) {
		t.Fatal("overnight silence must be healthy")
	}
	f.state = MarketState{Open: true}

	// 09:31 with no tick yet: measured from 09:30, so fresh, and no new call.
	calls := f.calls
	if !TickFresh(ctx, at(t, time.October, 15, 9, 31), last, maxAge, g) {
		t.Error("the first check after the open must not page on last night's silence")
	}
	if f.calls != calls {
		t.Errorf("a status call was made in the first minute after the open")
	}
	// 10:01, still nothing: now it is a real outage.
	if TickFresh(ctx, at(t, time.October, 15, 10, 1), last, maxAge, g) {
		t.Error("31 minutes into the session with no tick must be unhealthy")
	}
}

func TestFeedState_LiveClosedAndStale(t *testing.T) {
	ctx := context.Background()
	night := at(t, time.October, 14, 23, 0)
	last := at(t, time.October, 14, 16, 0)
	midday := at(t, time.October, 14, 11, 0)
	lastMorning := at(t, time.October, 14, 10, 0)

	if got := FeedState(ctx, midday, midday.Add(-time.Second), maxAge, newGate(t, &fakeStatus{state: MarketState{Open: true}})); got != FeedLive {
		t.Errorf("a tick a second old = %q, want live", got)
	}
	if got := FeedState(ctx, night, last, maxAge, newGate(t, &fakeStatus{state: MarketState{Open: false}})); got != FeedClosed {
		t.Errorf("seven hours of silence overnight = %q, want closed", got)
	}
	if got := FeedState(ctx, midday, lastMorning, maxAge, newGate(t, &fakeStatus{state: MarketState{Open: true}})); got != FeedStale {
		t.Errorf("an hour of silence mid-session = %q, want stale", got)
	}
}

// A lookup that fails is not "closed": a feed that cannot be vouched for must
// not read as one that is merely resting.
func TestFeedState_AFailedLookupIsStale(t *testing.T) {
	night := at(t, time.October, 14, 23, 0)
	last := at(t, time.October, 14, 16, 0)
	if got := FeedState(context.Background(), night, last, maxAge, newGate(t, &fakeStatus{err: errors.New("down")})); got != FeedStale {
		t.Errorf("a failed status lookup = %q, want stale", got)
	}
}

func TestFeedState_NilGateIsNeverClosed(t *testing.T) {
	now := at(t, time.October, 14, 12, 0)
	if got := FeedState(context.Background(), now, now.Add(-29*time.Minute), maxAge, nil); got != FeedLive {
		t.Errorf("29 minutes, no gate = %q, want live", got)
	}
	if got := FeedState(context.Background(), now, now.Add(-31*time.Minute), maxAge, nil); got != FeedStale {
		t.Errorf("31 minutes, no gate = %q, want stale (the simulator never goes quiet)", got)
	}
}

func TestFeedState_FlowingTicksMakeNoStatusCall(t *testing.T) {
	f := &fakeStatus{state: MarketState{Open: true}}
	g := newGate(t, f)
	now := at(t, time.October, 14, 12, 0)
	if FeedState(context.Background(), now, now.Add(-time.Second), maxAge, g) != FeedLive || f.calls != 0 {
		t.Errorf("calls = %d on a trading day with ticks flowing, want 0", f.calls)
	}
}

// The first check after the open must not count the night against the new
// session: it reads live, not stale, for the first half hour.
func TestFeedState_FirstCheckAfterTheOpenIsLive(t *testing.T) {
	ctx := context.Background()
	f := &fakeStatus{state: MarketState{Open: false}}
	g := newGate(t, f)
	last := at(t, time.October, 14, 16, 0)
	if got := FeedState(ctx, at(t, time.October, 14, 22, 0), last, maxAge, g); got != FeedClosed {
		t.Fatalf("overnight = %q, want closed", got)
	}
	f.state = MarketState{Open: true}
	if got := FeedState(ctx, at(t, time.October, 15, 9, 31), last, maxAge, g); got != FeedLive {
		t.Errorf("one minute after the open with no tick yet = %q, want live", got)
	}
	if got := FeedState(ctx, at(t, time.October, 15, 10, 15), last, maxAge, g); got != FeedStale {
		t.Errorf("forty-five minutes into the session with no tick = %q, want stale", got)
	}
}
