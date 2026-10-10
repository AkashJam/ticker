package reference

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// fireHour is the UTC hour of the daily run: after the ECB publishes (16:00
// CET the day before) and well before anyone reads the page.
const fireHour = 6

const (
	leaderWait = time.Minute
	leaderPoll = time.Second
)

// LeaderChecker is the ingest leader's lease. Only its holder fetches, so a
// second replica never repeats the day's calls.
type LeaderChecker interface{ IsLeader() bool }

// Pinger reports the outcome to a healthchecks.io check. sweep.HTTPPinger
// satisfies it; an empty URL there disables it.
type Pinger interface {
	Success(ctx context.Context)
	Fail(ctx context.Context, detail string)
}

// Job is one reference fetcher. Stale lets start-up catch-up skip a job whose
// stored data is already fresh; nil means always run.
type Job struct {
	Name  string
	Run   func(ctx context.Context) error
	Stale func(ctx context.Context) bool
}

// Scheduler runs every job once a day at 06:00 UTC, and once at start-up for
// any job whose data is stale. One check covers all jobs: a failure names the
// job in its detail.
type Scheduler struct {
	Jobs   []Job
	Leader LeaderChecker
	Ping   Pinger
	Now    func() time.Time
	Sleep  func(ctx context.Context, d time.Duration) error
	Log    *slog.Logger
}

func NewScheduler(jobs []Job, leader LeaderChecker, ping Pinger, log *slog.Logger) *Scheduler {
	return &Scheduler{Jobs: jobs, Leader: leader, Ping: ping, Now: time.Now, Sleep: sleepCtx, Log: log}
}

// NextFire returns the first 06:00 UTC strictly after now.
func NextFire(now time.Time) time.Time {
	n := now.UTC()
	c := time.Date(n.Year(), n.Month(), n.Day(), fireHour, 0, 0, 0, time.UTC)
	if !c.After(n) {
		c = c.AddDate(0, 0, 1)
	}
	return c
}

// Run blocks until ctx is canceled.
func (s *Scheduler) Run(ctx context.Context) {
	s.catchUp(ctx)
	for {
		next := NextFire(s.Now())
		s.Log.Info("reference: next run", "at", next)
		if err := s.Sleep(ctx, next.Sub(s.Now())); err != nil {
			return
		}
		if !s.Leader.IsLeader() {
			s.Log.Info("reference: not the leader, skipping")
			continue
		}
		s.runJobs(ctx, s.Jobs)
	}
}

func (s *Scheduler) catchUp(ctx context.Context) {
	if !s.waitForLeader(ctx) {
		return
	}
	var due []Job
	for _, j := range s.Jobs {
		if j.Stale == nil || j.Stale(ctx) {
			due = append(due, j)
		}
	}
	if len(due) > 0 {
		s.runJobs(ctx, due)
	}
}

func (s *Scheduler) runJobs(ctx context.Context, jobs []Job) {
	var failed []string
	for _, j := range jobs {
		if err := j.Run(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			s.Log.Error("reference: job failed", "job", j.Name, "error", err)
			failed = append(failed, fmt.Sprintf("%s: %v", j.Name, err))
		}
	}
	if len(failed) > 0 {
		s.Ping.Fail(ctx, strings.Join(failed, "; "))
		return
	}
	s.Ping.Success(ctx)
}

func (s *Scheduler) waitForLeader(ctx context.Context) bool {
	for waited := time.Duration(0); ; waited += leaderPoll {
		if s.Leader.IsLeader() {
			return true
		}
		if waited >= leaderWait {
			s.Log.Info("reference: not the leader, skipping catch-up")
			return false
		}
		if err := s.Sleep(ctx, leaderPoll); err != nil {
			return false
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
