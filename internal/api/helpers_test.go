package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// get serves one GET through the router and decodes a 200 body into v.
func get(t *testing.T, r http.Handler, path string, v any) int {
	t.Helper()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
	if v != nil && w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), v); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
	return w.Code
}
