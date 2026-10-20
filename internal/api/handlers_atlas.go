package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/atlas"
	"github.com/AkashJam/ticker/internal/ecb"
	"github.com/AkashJam/ticker/internal/fred"
	"github.com/AkashJam/ticker/internal/worldbank"
)

// memoTTL is how long a built response is reused. The datasets refresh once a
// day, so a minute costs nothing in freshness and saves re-parsing three blobs
// on every request.
const memoTTL = time.Minute

// memo holds the last response, as the bytes that were sent.
type memo struct {
	mu   sync.Mutex
	at   time.Time
	body []byte
}

func (m *memo) get(now time.Time, build func() ([]byte, error)) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.body != nil && now.Sub(m.at) < memoTTL {
		return m.body, nil
	}
	body, err := build()
	if err != nil {
		return nil, err
	}
	m.body, m.at = body, now
	return body, nil
}

// memoSet keeps one memo per key. Callers key it by a value taken from a
// reference table (a country slug, a canonical symbol), never by raw request
// input, so the map cannot outgrow that table.
type memoSet struct {
	mu sync.Mutex
	m  map[string]*memo
}

func (s *memoSet) of(key string) *memo {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = map[string]*memo{}
	}
	if s.m[key] == nil {
		s.m[key] = &memo{}
	}
	return s.m[key]
}

// GetAtlasCities handles GET /atlas/cities (§8): twenty-three rows, eight
// measures, every value with its source and as-of. It reads the reference
// store, which falls back to the embedded snapshots, so it answers with Redis
// down and with no vendor key, market hours or ticks.
func (h *Handlers) GetAtlasCities(c *gin.Context) {
	body, err := h.atlas.get(time.Now(), func() ([]byte, error) { return h.buildAtlas(c.Request.Context()) })
	if err != nil {
		h.Log.Error("api: atlas cities", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "atlas data unavailable"})
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", body)
}

func (h *Handlers) buildAtlas(ctx context.Context) ([]byte, error) {
	if h.Ref == nil {
		return nil, fmt.Errorf("no reference store")
	}
	countries, err := h.Ref.AtlasCountries(ctx)
	if err != nil {
		return nil, err
	}
	var (
		f fred.Dataset
		e ecb.Dataset
		w worldbank.Dataset
	)
	for name, v := range map[string]any{fred.DatasetName: &f, ecb.DatasetName: &e, worldbank.DatasetName: &w} {
		if _, err := h.Ref.Get(ctx, name, v); err != nil {
			return nil, err
		}
	}
	resp, err := atlas.Build(countries, f, e, w)
	if err != nil {
		return nil, err
	}
	return json.Marshal(resp)
}
