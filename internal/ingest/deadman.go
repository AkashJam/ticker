package ingest

import (
	"context"
	"net/http"
	"time"
)

// pingInterval is how often DeadMan checks health and, if healthy, pings.
// healthchecks.io only needs to hear from us well inside its own alert
// window, not on every single tick.
const pingInterval = 60 * time.Second

// DeadMan is the dead-man switch (portfolio.md §13): a periodic heartbeat to
// an external monitor (healthchecks.io), so a stuck or crashed process gets
// alerted on even though nothing about a silently-stopped process trips an
// in-process health check. A zero-value url disables it entirely — not every
// environment (e.g. local dev) has one configured.
//
// The heartbeat runs on its own timer, deliberately decoupled from tick
// flow: a quiet upstream or a leader failover isn't an outage, so it must
// not look like one to the monitor. Liveness is instead decided by the
// healthy callback passed to Run.
type DeadMan struct {
	url      string
	client   *http.Client
	interval time.Duration
}

func NewDeadMan(url string) *DeadMan {
	return &DeadMan{url: url, client: &http.Client{Timeout: 5 * time.Second}, interval: pingInterval}
}

// Run pings every interval while healthy reports true, until ctx is
// canceled. It returns immediately when disabled (nil receiver or empty
// URL). Failed pings are silently swallowed — that's the point of a
// dead-man switch: healthchecks.io alerts on a *missing* ping, we don't
// need to also alert on a failed one here.
func (d *DeadMan) Run(ctx context.Context, healthy func(context.Context) bool) {
	if d == nil || d.url == "" {
		return
	}

	t := time.NewTicker(d.interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if healthy(ctx) {
				d.send(ctx)
			}
		}
	}
}

func (d *DeadMan) send(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, d.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.url, nil)
	if err != nil {
		return
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
}
