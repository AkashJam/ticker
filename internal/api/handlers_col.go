package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/source"
)

// GetCostOfLiving handles GET /col (§8) — the sim source's Cost-of-Living
// family, computed fresh on every read (§6.3: not a table).
func (h *Handlers) GetCostOfLiving(c *gin.Context) {
	c.JSON(http.StatusOK, source.CostOfLiving())
}

// GetCostOfLivingCity handles GET /col/{city} (§8).
func (h *Handlers) GetCostOfLivingCity(c *gin.Context) {
	city := c.Param("city")
	detail, ok := source.CostOfLivingByCity(city)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "unknown city"})
		return
	}
	c.JSON(http.StatusOK, detail)
}
