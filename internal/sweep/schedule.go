package sweep

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// LeaderChecker is the ingest leader's lease. Only its holder sweeps, so a
// second replica never repeats the day's 28 calls.
type LeaderChecker interface{ IsLeader() bool }

// leaderWait bounds how long start-up catch-up waits to learn whether this
// instance holds the lease (the leader loop acquires within a few seconds).
const (
	leaderWait = time.Minute
	leaderPoll = time.Second
)

// Scheduler runs the sweep every weekday at 16:15 New York time, and once at
// start-up if the last completed session has no bar.
type Scheduler struct {
	Runner *Runner
	Leader LeaderChecker
	Ping   Pinger
	Loc    *time.Location
	Now    func() time.Time
	Sleep  func(ctx context.Context, d time.Duration) error
	Log    *slog.Logger
}

func NewScheduler(r *Runner, leader LeaderChecker, ping Pinger, log *slog.Logger) *Scheduler {
	return &Scheduler{Runner: r, Leader: leader, Ping: ping, Loc: r.Loc, Now: time.Now, Sleep: sleepCtx, Log: log}
}

// NextFire returns the first weekday 16:15 in loc strictly after now. It
// builds each candidate with time.Date in loc, so the clock change on 1
// November 2026 (US) moves the UTC instant and not the New York time. It does
// not know exchange holidays — the runner finds those out from the quote.
func NextFire(now time.Time, loc *time.Location) time.Time {
	local := now.In(loc)
	y, m, d := local.Date()
	for i := 0; i < 8; i++ {
		c := time.Date(y, m, d+i, fireHour, fireMinute, 0, 0, loc)
		if wd := c.Weekday(); wd == time.Saturday || wd == time.Sunday {
			continue
		}
		if c.After(now) {
			return c
		}
	}
	panic("sweep: no weekday within 8 days") // unreachable
}

// Run blocks until ctx is canceled.
func (s *Scheduler) Run(ctx context.Context) {
	s.catchUp(ctx)
	for {
		next := NextFire(s.Now(), s.Loc)
		s.Log.Info("sweep: next run", "at", next)
		if err := s.Sleep(ctx, next.Sub(s.Now())); err != nil {
			return
		}
		s.fire(ctx)
	}
}

func (s *Scheduler) catchUp(ctx context.Context) {
	if !s.waitForLeader(ctx) {
		return
	}
	res, err := s.Runner.Run(ctx, ModeCatchUp)
	if err != nil {
		s.Log.Error("sweep: catch-up", "error", err)
		return
	}
	s.Log.Info("sweep: catch-up", "state", res.State, "session", res.Session.Format(time.DateOnly), "written", res.Written)
	// Report only when catch-up actually did the day's work; "nothing to do"
	// is not the scheduled run's success to claim.
	if res.State == StateWritten || res.State == StateFailed {
		s.report(ctx, res)
	}
}

func (s *Scheduler) fire(ctx context.Context) {
	if !s.Leader.IsLeader() {
		s.Log.Info("sweep: not the leader, skipping")
		return
	}
	res, err := s.Runner.Run(ctx, ModeScheduled)
	if err != nil {
		if ctx.Err() == nil {
			s.Log.Error("sweep: run", "error", err)
			s.Ping.Fail(ctx, err.Error())
		}
		return
	}
	s.Log.Info("sweep: run", "state", res.State, "session", res.Session.Format(time.DateOnly), "written", res.Written, "failed", res.Failed)
	s.report(ctx, res)
}

// report maps an outcome to the check. A holiday is a correct skip and counts
// as success; anything that left a symbol unwritten is a failure.
func (s *Scheduler) report(ctx context.Context, res Result) {
	switch res.State {
	case StateWritten, StateHoliday:
		s.Ping.Success(ctx)
	default:
		s.Ping.Fail(ctx, fmt.Sprintf("sweep %s: state=%s written=%d failed=%s",
			res.Session.Format(time.DateOnly), res.State, res.Written, strings.Join(res.Failed, ",")))
	}
}

func (s *Scheduler) waitForLeader(ctx context.Context) bool {
	for waited := time.Duration(0); ; waited += leaderPoll {
		if s.Leader.IsLeader() {
			return true
		}
		if waited >= leaderWait {
			s.Log.Info("sweep: not the leader, skipping catch-up")
			return false
		}
		if err := s.Sleep(ctx, leaderPoll); err != nil {
			return false
		}
	}
}
