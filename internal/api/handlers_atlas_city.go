package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/atlas"
	"github.com/AkashJam/ticker/internal/ecb"
	"github.com/AkashJam/ticker/internal/history"
	"github.com/AkashJam/ticker/internal/worldbank"
)

// GetAtlasCity handles GET /atlas/cities/{city} (§8): one city's page data, by
// the country's slug. Like the comparison it reads the reference store, which
// falls back to the embedded snapshots, so it answers with Redis down. A slug
// that is not an Atlas country with a survey is a 404. A city whose record is not
// yet verified is still served, with verified false: the page ships on
// hasCityPage, which the portfolio reads from /atlas/cities.
func (h *Handlers) GetAtlasCity(c *gin.Context) {
	slug := strings.ToLower(c.Param("city"))
	if h.Ref == nil {
		h.Log.Error("api: atlas city: no reference store")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "atlas data unavailable"})
		return
	}
	// Resolve the slug against the table first, so an arbitrary string never
	// becomes a memo key.
	ok, err := h.cityExists(c.Request.Context(), slug)
	if err != nil {
		h.Log.Error("api: atlas city", "city", slug, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "atlas data unavailable"})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "unknown city"})
		return
	}
	body, err := h.cities.of(slug).get(time.Now(), func() ([]byte, error) { return h.buildCity(c.Request.Context(), slug) })
	if err != nil {
		h.Log.Error("api: atlas city", "city", slug, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "atlas data unavailable"})
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", body)
}

func (h *Handlers) cityExists(ctx context.Context, slug string) (bool, error) {
	countries, err := h.Ref.AtlasCountries(ctx)
	if err != nil {
		return false, err
	}
	for _, c := range countries {
		if c.Slug == slug {
			_, found, err := h.Ref.Survey(ctx, slug)
			return found, err
		}
	}
	return false, nil
}

func (h *Handlers) buildCity(ctx context.Context, slug string) ([]byte, error) {
	var in atlas.CityInput
	var err error
	if in.Countries, err = h.Ref.AtlasCountries(ctx); err != nil {
		return nil, err
	}
	if in.Surveys, err = h.Ref.Surveys(ctx); err != nil {
		return nil, err
	}
	for name, v := range map[string]any{history.DatasetName: &in.History, ecb.DatasetName: &in.ECB, worldbank.DatasetName: &in.WB} {
		if _, err := h.Ref.Get(ctx, name, v); err != nil {
			return nil, err
		}
	}
	resp, err := atlas.BuildCity(slug, in)
	if err != nil {
		if errors.Is(err, atlas.ErrNoCity) {
			return nil, fmt.Errorf("city %s vanished between lookup and build: %w", slug, err)
		}
		return nil, err
	}
	return json.Marshal(resp)
}
