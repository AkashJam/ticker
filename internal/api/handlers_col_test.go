package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/change"
	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/worldbank"
)

// embeddedCPI is how the server backs /col: the reference store, here with no
// Redis at all, so it answers from the embedded World Bank snapshot.
func embeddedCPI() source.CPIFunc {
	st := reference.New(nil, reference.Embedded(), nil)
	return func(ctx context.Context, code string) (change.Series, bool) {
		var ds worldbank.Dataset
		if _, err := st.Get(ctx, worldbank.DatasetName, &ds); err != nil {
			return change.Series{}, false
		}
		row, ok := ds.Rows[code]
		return row.Prices.Series, ok
	}
}

func colRouter(h *Handlers) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/col", h.GetCostOfLiving)
	r.GET("/col/:city", h.GetCostOfLivingCity)
	return r
}

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

func TestColStillServesAfterAnnualInflateIsGone(t *testing.T) {
	r := colRouter(&Handlers{CPI: embeddedCPI()})

	var list []source.CostOfLivingSnapshot
	if code := get(t, r, "/col", &list); code != http.StatusOK || len(list) != 5 {
		t.Fatalf("GET /col = %d with %d cities, want 200 and 5", code, len(list))
	}
	byCity := map[string]source.CostOfLivingSnapshot{}
	for _, s := range list {
		byCity[s.City] = s
		if !s.Simulated || s.LocalValue <= 0 || s.EurValue <= 0 {
			t.Errorf("%+v", s)
		}
	}
	// Milan's 2024 survey is carried to 2025 on Italy's CPI; New York's is a
	// 2025 survey and the US has no 2025 CPI, so it stays as published.
	if m := byCity["Milan"]; m.LocalValue <= 972 || m.LocalValue > 1000 || m.ChangeYoY <= 0 {
		t.Errorf("Milan = %+v, want 972 carried a little way forward", m)
	}
	if ny := byCity["New York"]; ny.LocalValue != 4833 || ny.ChangeYoY != 0 {
		t.Errorf("New York = %+v, want the published 4833", ny)
	}

	var detail source.CostOfLivingDetail
	if code := get(t, r, "/col/Milan", &detail); code != http.StatusOK || len(detail.Series) != 2 || len(detail.Basket) == 0 {
		t.Fatalf("GET /col/Milan = %d, series %d, basket %d", code, len(detail.Series), len(detail.Basket))
	}
	if code := get(t, r, "/col/Atlantis", nil); code != http.StatusNotFound {
		t.Errorf("unknown city = %d, want 404", code)
	}
}

func TestColWithoutCPIServesBaselines(t *testing.T) {
	var list []source.CostOfLivingSnapshot
	if code := get(t, colRouter(&Handlers{}), "/col", &list); code != http.StatusOK || len(list) != 5 {
		t.Fatalf("GET /col = %d with %d cities", code, len(list))
	}
	for _, s := range list {
		if s.City == "London" && s.LocalValue != 2333 {
			t.Errorf("London = %v, want the published 2333", s.LocalValue)
		}
	}
}
