package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/trigger"
)

// GetTape handles GET /market/tape (§8): the Prices table's two history columns
// for the eighteen sensors, a sparkline of the last sixty closes and the z on
// the last close, "sigma, last close" (phase15.md step 7, decision 3). It is
// not part of GET /sensors, which is reference data and caches as such.
func (h *Handlers) GetTape(c *gin.Context) {
	body, err := h.tape.get(time.Now(), func() ([]byte, error) { return h.buildTape(c.Request.Context()) })
	if err != nil {
		h.Log.Error("api: tape", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "tape unavailable"})
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", body)
}

func (h *Handlers) buildTape(ctx context.Context) ([]byte, error) {
	if h.Candles == nil {
		return nil, fmt.Errorf("the tape is not wired")
	}
	all, err := trigger.LoadAsOf(ctx, h.Candles, source.SweepSymbolCodes(), time.Now().UTC())
	if err != nil {
		return nil, err
	}
	var sensors []string
	for _, s := range source.StreamUniverse() {
		sensors = append(sensors, s.Symbol)
	}
	t := trigger.BuildTape(all, sensors)
	type response struct {
		AsOf    *string              `json:"asOf"`
		Sensors []trigger.TapeSensor `json:"sensors"`
	}
	out := response{Sensors: t.Sensors}
	if !t.AsOf.IsZero() {
		s := t.AsOf.Format(time.DateOnly)
		out.AsOf = &s
	}
	return json.Marshal(out)
}
