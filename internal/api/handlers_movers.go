package api

import (
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/source"
)

type moverResponse struct {
	Symbol        string  `json:"symbol"`
	ChangePercent float64 `json:"changePercent"`
	Price         float64 `json:"price"`
}

type moversResponse struct {
	Gainers []moverResponse `json:"gainers"`
	Losers  []moverResponse `json:"losers"`
}

const moversTopN = 5

// GetMovers handles GET /market/movers (§8) — computed from every tracked
// SIM: symbol's current snapshot, not stored separately.
func (h *Handlers) GetMovers(c *gin.Context) {
	ctx := c.Request.Context()

	movers := make([]moverResponse, 0, len(source.Symbols))
	for _, s := range source.Symbols {
		snap, err := h.snapshotFor(ctx, s.Symbol)
		if err != nil {
			h.Log.Error("api: movers: snapshot", "symbol", s.Symbol, "error", err)
			continue // one symbol's failure shouldn't fail the whole movers list
		}
		movers = append(movers, moverResponse{Symbol: snap.Symbol, ChangePercent: snap.ChangePercent, Price: snap.Price})
	}

	gainers := append([]moverResponse(nil), movers...)
	sort.Slice(gainers, func(i, j int) bool { return gainers[i].ChangePercent > gainers[j].ChangePercent })
	gainers = topN(gainers, moversTopN)

	losers := append([]moverResponse(nil), movers...)
	sort.Slice(losers, func(i, j int) bool { return losers[i].ChangePercent < losers[j].ChangePercent })
	losers = topN(losers, moversTopN)

	c.JSON(http.StatusOK, moversResponse{Gainers: gainers, Losers: losers})
}

func topN(items []moverResponse, n int) []moverResponse {
	if len(items) > n {
		return items[:n]
	}
	return items
}
