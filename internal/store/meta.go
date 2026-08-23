package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AkashJam/ticker/internal/source"
)

// Meta is the symbols/metadata repository (portfolio.md §6.3) — a distinct
// type from Timescale even though v1 shares the same pool, per §6.3's own
// note that this seam is what lets metadata split onto its own instance
// later at zero code cost.
type Meta struct {
	pool *pgxpool.Pool
}

func NewMeta(pool *pgxpool.Pool) *Meta {
	return &Meta{pool: pool}
}

// Symbols returns every tracked symbol — the shape GET /symbols serves.
func (m *Meta) Symbols(ctx context.Context) ([]source.SymbolInfo, error) {
	const q = `
		SELECT symbol, name, type, exchange
		FROM symbols
		WHERE tracked = true
		ORDER BY symbol`
	rows, err := m.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: query symbols: %w", err)
	}
	defer rows.Close()

	// Non-nil for the same reason as store/timescale.go's Candles/
	// RecentCandles — a nil slice marshals to JSON `null`, breaking the
	// documented §8 array contract for GET /symbols.
	out := make([]source.SymbolInfo, 0)
	for rows.Next() {
		var s source.SymbolInfo
		if err := rows.Scan(&s.Symbol, &s.Name, &s.Type, &s.Exchange); err != nil {
			return nil, fmt.Errorf("store: scan symbol: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
