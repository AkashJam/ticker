package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// GetSensors handles GET /sensors (§8): the eighteen market sensors in their
// fixed display order. Like /atlas/cities it reads the reference store, which
// falls back to the embedded snapshot, so it answers with Redis down and with
// no vendor key or market hours.
func (h *Handlers) GetSensors(c *gin.Context) {
	body, err := h.sensors.get(time.Now(), func() ([]byte, error) { return h.buildSensors(c.Request.Context()) })
	if err != nil {
		h.Log.Error("api: sensors", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "sensor list unavailable"})
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", body)
}

func (h *Handlers) buildSensors(ctx context.Context) ([]byte, error) {
	if h.Ref == nil {
		return nil, fmt.Errorf("no reference store")
	}
	sensors, err := h.Ref.Sensors(ctx)
	if err != nil {
		return nil, err
	}
	return json.Marshal(sensors)
}
