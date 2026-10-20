package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
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

// Exists reports whether symbol is in the table: the four simulator rows and
// the 28 sweep symbols. GET /sensors lists the sensors; this only decides
// whether a symbol's pages and daily bars are served.
func (m *Meta) Exists(ctx context.Context, symbol string) (bool, error) {
	var ok bool
	if err := m.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM symbols WHERE symbol = $1)`, symbol).Scan(&ok); err != nil {
		return false, fmt.Errorf("store: symbol exists: %w", err)
	}
	return ok, nil
}
