package ingest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func runDeadMan(t *testing.T, url string, healthy bool, d time.Duration) {
	t.Helper()
	dm := NewDeadMan(url)
	dm.interval = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	done := make(chan struct{})
	go func() {
		dm.Run(ctx, func(context.Context) bool { return healthy })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d + time.Second):
		t.Fatal("Run did not exit after ctx cancel")
	}
}

func countingServer(hits *atomic.Int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
}

func TestDeadMan_PingsWhileHealthy(t *testing.T) {
	var hits atomic.Int32
	srv := countingServer(&hits)
	defer srv.Close()

	runDeadMan(t, srv.URL, true, 100*time.Millisecond)

	if hits.Load() < 2 {
		t.Errorf("expected repeated pings while healthy, got %d", hits.Load())
	}
}

func TestDeadMan_SilentWhenUnhealthy(t *testing.T) {
	var hits atomic.Int32
	srv := countingServer(&hits)
	defer srv.Close()

	runDeadMan(t, srv.URL, false, 100*time.Millisecond)

	if hits.Load() != 0 {
		t.Errorf("expected no pings while unhealthy, got %d", hits.Load())
	}
}

func TestDeadMan_DisabledWithEmptyURL(t *testing.T) {
	// Returns immediately rather than blocking until ctx expires.
	dm := NewDeadMan("")
	done := make(chan struct{})
	go func() {
		dm.Run(context.Background(), func(context.Context) bool { return true })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disabled DeadMan.Run should return immediately")
	}
}
