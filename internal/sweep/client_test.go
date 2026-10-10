package sweep

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testClient(t *testing.T, h http.HandlerFunc) *FinnhubClient {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c := NewFinnhubClient("secret-key")
	c.baseURL = srv.URL
	return c
}

func TestClient_QuoteParsesAndAuthenticatesByHeader(t *testing.T) {
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Finnhub-Token"); got != "secret-key" {
			t.Errorf("X-Finnhub-Token = %q, want the key", got)
		}
		if strings.Contains(r.URL.String(), "secret-key") || r.URL.Query().Get("token") != "" {
			t.Errorf("the key must never be in the URL, got %s", r.URL)
		}
		if r.URL.Path != "/quote" || r.URL.Query().Get("symbol") != "RY" {
			t.Errorf("unexpected request %s", r.URL)
		}
		_, _ = w.Write([]byte(`{"c":191.91,"d":1.35,"dp":0.7,"h":192.18,"l":189.63,"o":190.34,"pc":190.56,"t":1791576000}`))
	})

	q, err := c.Quote(context.Background(), "RY")
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 10, 9, 20, 0, 0, 0, time.UTC)
	if q.Current != 191.91 || q.Open != 190.34 || q.High != 192.18 || q.Low != 189.63 || q.PrevClose != 190.56 || !q.At.Equal(want) {
		t.Errorf("quote = %+v", q)
	}
}

func TestClient_UnknownSymbolIsErrNoData(t *testing.T) {
	c := testClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"c":0,"d":null,"dp":null,"h":0,"l":0,"o":0,"pc":0,"t":0}`))
	})
	if _, err := c.Quote(context.Background(), "NOPE"); !errors.Is(err, ErrNoData) {
		t.Errorf("err = %v, want ErrNoData", err)
	}
}

func TestClient_StatusErrors(t *testing.T) {
	tests := []struct {
		name       string
		code       int
		retryAfter string
		wantRetry  bool
		wantWait   time.Duration
	}{
		{"unauthorized is final", http.StatusUnauthorized, "", false, 0},
		{"forbidden is final", http.StatusForbidden, "", false, 0},
		{"rate limited retries and keeps the wait", http.StatusTooManyRequests, "7", true, 7 * time.Second},
		{"server error retries", http.StatusBadGateway, "", true, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := testClient(t, func(w http.ResponseWriter, _ *http.Request) {
				if tt.retryAfter != "" {
					w.Header().Set("Retry-After", tt.retryAfter)
				}
				w.WriteHeader(tt.code)
			})
			_, err := c.Quote(context.Background(), "RY")
			var se *StatusError
			if !errors.As(err, &se) {
				t.Fatalf("err = %v, want *StatusError", err)
			}
			if se.Code != tt.code || se.Retryable() != tt.wantRetry || se.RetryAfter != tt.wantWait {
				t.Errorf("got %+v retryable=%v", se, se.Retryable())
			}
		})
	}
}
