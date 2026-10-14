package sweep

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPinger(t *testing.T) {
	type hit struct{ method, path, body string }
	var hits []hit
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		hits = append(hits, hit{r.Method, r.URL.Path, string(b)})
	}))
	defer srv.Close()

	p := NewPinger(srv.URL+"/abc-123/", discardLog())
	p.Success(context.Background())
	p.Fail(context.Background(), "failed: KB")

	want := []hit{{"GET", "/abc-123", ""}, {"POST", "/abc-123/fail", "failed: KB"}}
	if len(hits) != 2 || hits[0] != want[0] || hits[1] != want[1] {
		t.Errorf("hits = %+v, want %+v", hits, want)
	}
}

func TestPinger_LogsRejectedPing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound) // healthchecks.io's answer for an unknown UUID
	}))
	defer srv.Close()

	var logged strings.Builder
	p := NewPinger(srv.URL+"/wrong-uuid", slog.New(slog.NewTextHandler(&logged, nil)))
	p.Success(context.Background())

	if out := logged.String(); !strings.Contains(out, "sweep: ping rejected") || !strings.Contains(out, "status=404") {
		t.Errorf("log = %q, want a rejected-ping warning with status=404", out)
	}
	if strings.Contains(logged.String(), "wrong-uuid") {
		t.Error("the ping URL leaked into the log")
	}
}

func TestPinger_EmptyURLAndNilAreNoOps(_ *testing.T) {
	NewPinger("", discardLog()).Success(context.Background())
	NewPinger("", discardLog()).Fail(context.Background(), "x")
	var nilP *HTTPPinger
	nilP.Success(context.Background())
}
