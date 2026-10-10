package reference

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type fakeLeader bool

func (f fakeLeader) IsLeader() bool { return bool(f) }

type fakePing struct {
	ok    int
	fails []string
}

func (p *fakePing) Success(context.Context)          { p.ok++ }
func (p *fakePing) Fail(_ context.Context, d string) { p.fails = append(p.fails, d) }

func testScheduler(leader bool, ping Pinger, jobs ...Job) *Scheduler {
	s := NewScheduler(jobs, fakeLeader(leader), ping, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.Sleep = func(context.Context, time.Duration) error { return nil }
	return s
}

func TestNextFire(t *testing.T) {
	at := func(h int) time.Time { return time.Date(2026, 10, 10, h, 0, 0, 0, time.UTC) }
	if got := NextFire(at(5)); !got.Equal(at(6)) {
		t.Errorf("before 06:00 = %v", got)
	}
	if got := NextFire(at(6)); !got.Equal(at(6).AddDate(0, 0, 1)) {
		t.Errorf("at 06:00 = %v, want tomorrow (strictly after)", got)
	}
}

func TestCatchUpRunsOnlyStaleJobsAndPingsOnce(t *testing.T) {
	var ran []string
	job := func(name string, stale bool) Job {
		return Job{Name: name, Stale: func(context.Context) bool { return stale },
			Run: func(context.Context) error { ran = append(ran, name); return nil }}
	}
	p := &fakePing{}
	testScheduler(true, p, job("fresh", false), job("old", true)).catchUp(context.Background())
	if len(ran) != 1 || ran[0] != "old" || p.ok != 1 {
		t.Errorf("ran=%v ok=%d, want only the stale job and one success ping", ran, p.ok)
	}

	p = &fakePing{}
	testScheduler(true, p, job("fresh", false)).catchUp(context.Background())
	if p.ok != 0 {
		t.Error("catch-up with nothing to do claimed a success")
	}
}

func TestCatchUpSkipsWhenNotLeader(t *testing.T) {
	ran := false
	p := &fakePing{}
	testScheduler(false, p, Job{Name: "x", Run: func(context.Context) error { ran = true; return nil }}).catchUp(context.Background())
	if ran || p.ok != 0 {
		t.Error("a non-leader fetched")
	}
}

func TestFailureNamesTheJobAndOtherJobsStillRun(t *testing.T) {
	var ran []string
	p := &fakePing{}
	jobs := []Job{
		{Name: "fred", Run: func(context.Context) error { return errors.New("2 series failed") }},
		{Name: "ecb", Run: func(context.Context) error { ran = append(ran, "ecb"); return nil }},
	}
	testScheduler(true, p, jobs...).runJobs(context.Background(), jobs)
	if len(ran) != 1 || p.ok != 0 || len(p.fails) != 1 || !strings.Contains(p.fails[0], "fred: 2 series failed") {
		t.Errorf("ran=%v ok=%d fails=%v", ran, p.ok, p.fails)
	}
}
