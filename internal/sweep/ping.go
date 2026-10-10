package sweep

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// Pinger reports the sweep's outcome to its own healthchecks.io check,
// separate from the ingest dead-man so a missed sweep pages on its own.
type Pinger interface {
	Success(ctx context.Context)
	Fail(ctx context.Context, detail string)
}

// HTTPPinger pings a healthchecks.io check URL. An empty URL disables it.
// Failures are logged, never returned: a monitor being unreachable must not
// stop the job it watches.
type HTTPPinger struct {
	url    string
	client *http.Client
	log    *slog.Logger
}

func NewPinger(url string, log *slog.Logger) *HTTPPinger {
	return &HTTPPinger{url: strings.TrimRight(url, "/"), client: &http.Client{Timeout: 5 * time.Second}, log: log}
}

func (p *HTTPPinger) Success(ctx context.Context) {
	if p == nil {
		return
	}
	p.send(ctx, p.url, "")
}

// Fail pings the check's /fail endpoint with detail as the body, so the alert
// names the symbols that did not land.
func (p *HTTPPinger) Fail(ctx context.Context, detail string) {
	if p == nil {
		return
	}
	p.send(ctx, p.url+"/fail", detail)
}

func (p *HTTPPinger) send(ctx context.Context, target, body string) {
	if p.url == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, p.client.Timeout)
	defer cancel()

	method := http.MethodGet
	if body != "" {
		method = http.MethodPost
	}
	req, err := http.NewRequestWithContext(ctx, method, target, strings.NewReader(body))
	if err != nil {
		p.log.Warn("sweep: ping request", "error", err)
		return
	}
	resp, err := p.client.Do(req)
	if err != nil {
		p.log.Warn("sweep: ping failed", "error", err)
		return
	}
	_ = resp.Body.Close()
}
