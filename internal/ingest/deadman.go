package ingest

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// pingInterval throttles how often DeadMan actually calls out, even if
// Ping is invoked on every tick (several times a second) — healthchecks.io
// only needs to hear from us well inside its own alert window, not on
// every single tick.
const pingInterval = 60 * time.Second

// DeadMan is the dead-man switch (portfolio.md §13): a periodic ping from
// the ingestion loop to an external monitor (healthchecks.io), so a stuck
// or crashed ingest loop gets alerted on even though nothing about a
// silently-stopped process trips an in-process health check. A zero-value
// url disables it entirely — not every environment (e.g. local dev) has
// one configured.
type DeadMan struct {
	url    string
	client *http.Client

	mu   sync.Mutex
	last time.Time
}

func NewDeadMan(url string) *DeadMan {
	return &DeadMan{url: url, client: &http.Client{Timeout: 5 * time.Second}}
}

// Ping fires a best-effort GET to the configured URL, throttled to at most
// once per pingInterval. Failures are silently swallowed — that's the
// point of a dead-man switch: healthchecks.io alerts on a *missing* ping,
// we don't need to also alert on a failed one here.
//
// ping runs fire-and-forget in its own goroutine so callers on the hot
// tick path never block on a network call, and the caller's ctx (scoped to
// one tick) would cancel this before the request could even complete.
//
//nolint:contextcheck // deliberately doesn't accept the caller's ctx: the
func (d *DeadMan) Ping() {
	if d == nil || d.url == "" {
		return
	}

	d.mu.Lock()
	if time.Since(d.last) < pingInterval {
		d.mu.Unlock()
		return
	}
	d.last = time.Now()
	d.mu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), d.client.Timeout)
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
	}()
}
