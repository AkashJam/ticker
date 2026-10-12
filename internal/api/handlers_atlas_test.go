package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/atlas"
	"github.com/AkashJam/ticker/internal/reference"
)

func atlasRouter(h *Handlers) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/atlas/cities", h.GetAtlasCities)
	return r
}

func testHandlers(ref *reference.Store) *Handlers {
	return &Handlers{Ref: ref, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

// The phase gate: with no Redis at all, the route answers from the embedded
// snapshots.
func TestAtlasCitiesAnswersWithRedisDown(t *testing.T) {
	h := testHandlers(reference.New(nil, reference.Embedded(), nil))
	var resp atlas.Response
	if code := get(t, atlasRouter(h), "/atlas/cities", &resp); code != http.StatusOK {
		t.Fatalf("GET /atlas/cities = %d", code)
	}
	if resp.End != "2024" || len(resp.Rows) != 23 || len(resp.Windows) != 3 {
		t.Fatalf("end %s, %d rows, windows %v", resp.End, len(resp.Rows), resp.Windows)
	}
	by := map[string]atlas.Row{}
	for _, r := range resp.Rows {
		by[r.Slug] = r
		if r.Slug == "" || r.HasCityPage {
			t.Errorf("%s: slug %q hasCityPage %v", r.Code, r.Slug, r.HasCityPage)
		}
	}
	it := by["italy"]
	if it.City != "Milan" || it.Sensor != nil || it.Region != "europe" {
		t.Errorf("Italy = %+v", it)
	}
	if *it.Shares.Change["2010"] != 69.8 || *it.Housing.Change["2010"] != -26 || *it.Prices.Change["2010"] != 29.9 {
		t.Errorf("Italy 2010 changes: %v %v %v", *it.Shares.Change["2010"], *it.Housing.Change["2010"], *it.Prices.Change["2010"])
	}
	if it.Currency.Reason != "euro" || it.Currency.Change != nil {
		t.Errorf("Italy currency = %+v", it.Currency)
	}
	if cl := by["chile"].Currency; cl.Derived != "via USD" || cl.Change["2010"] == nil {
		t.Errorf("Chile currency = %+v", cl)
	}
	if il := by["israel"].Currency; il.Change["2010"] != nil || il.Change["2016"] == nil || il.Reason != "unpublished" {
		t.Errorf("Israel currency = %+v", il)
	}
	if by["japan"].Sensor == nil || *by["japan"].Sensor != "MUFG" {
		t.Errorf("Japan sensor = %v", by["japan"].Sensor)
	}
}

func TestAtlasCitiesJSONShape(t *testing.T) {
	h := testHandlers(reference.New(nil, reference.Embedded(), nil))
	w := httptest.NewRecorder()
	atlasRouter(h).ServeHTTP(w, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/atlas/cities", nil))
	var raw struct {
		End  string                       `json:"end"`
		Rows []map[string]json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("content type = %s", ct)
	}
	// The keys the Zod schemas in portfolio/lib/market-schemas.ts expect.
	want := []string{"slug", "city", "country", "code", "sensor", "hasCityPage", "region",
		"shares", "housing", "prices", "currency", "everydaySpendingEur", "gniPpp", "unemployment", "lifeExpectancy"}
	for _, row := range raw.Rows {
		for _, k := range want {
			if _, ok := row[k]; !ok {
				t.Fatalf("row is missing %q", k)
			}
		}
		for _, k := range []string{"inflation", "composition", "costOfLiving"} {
			if _, ok := row[k]; ok {
				t.Errorf("row has %q, a city-page figure", k)
			}
		}
	}
}

func TestAtlasCitiesIsMemoisedAndAnErrorIs500(t *testing.T) {
	h := testHandlers(reference.New(nil, reference.Embedded(), nil))
	r := atlasRouter(h)
	if code := get(t, r, "/atlas/cities", nil); code != http.StatusOK {
		t.Fatal(code)
	}
	first := h.atlas.body
	h.Ref = nil // a second call inside the TTL must not touch the store
	if code := get(t, r, "/atlas/cities", nil); code != http.StatusOK || &first[0] != &h.atlas.body[0] {
		t.Errorf("second call = %d, want the memoised body", code)
	}

	h.atlas.at = time.Now().Add(-2 * atlasTTL) // expired, and there is no store
	if code := get(t, r, "/atlas/cities", nil); code != http.StatusInternalServerError {
		t.Errorf("no store = %d, want 500", code)
	}
}
