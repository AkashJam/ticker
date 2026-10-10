package sweep

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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

func TestPinger_EmptyURLAndNilAreNoOps(_ *testing.T) {
	NewPinger("", discardLog()).Success(context.Background())
	NewPinger("", discardLog()).Fail(context.Background(), "x")
	var nilP *HTTPPinger
	nilP.Success(context.Background())
}
