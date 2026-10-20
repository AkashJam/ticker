// Package history stores the whole record behind the city pages' charts (Phase 16
// step 3): the OECD share index and BIS house prices from each series' first
// observation, and the ECB's monthly rates from 1999. The "fred" and "ecb"
// datasets /atlas reads start in January 2010 and are left exactly as they are;
// this is a second dataset beside them, so a city page can draw 1957 without
// moving a number on /atlas (decision 2).
package history

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/AkashJam/ticker/internal/ecb"
	"github.com/AkashJam/ticker/internal/fred"
	"github.com/AkashJam/ticker/internal/reference"
)

// DatasetName is the reference-store key and embedded file.
const DatasetName = "history"

// Row is one country's two long series.
type Row struct {
	Shares  fred.Measure `json:"shares"`
	Housing fred.Measure `json:"housing"`
}

// Dataset is the blob stored as "history".
type Dataset struct {
	GeneratedAt string                  `json:"generatedAt"`
	Rows        map[string]Row          `json:"rows"` // by ISO alpha-2
	Currencies  map[string]ecb.Currency `json:"currencies"`
}

// Refresher builds the dataset from the same two fetchers /atlas uses.
type Refresher struct {
	Fred  *fred.Refresher
	ECB   *ecb.Refresher
	Store *reference.Store
	Now   func() time.Time
	Log   *slog.Logger
}

func NewRefresher(f *fred.Refresher, e *ecb.Refresher, st *reference.Store, log *slog.Logger) *Refresher {
	return &Refresher{Fred: f, ECB: e, Store: st, Now: time.Now, Log: log}
}

// Build fetches every series. It is all or nothing, as fred's is: a partial
// blob would leave countries on different ends.
func (r *Refresher) Build(ctx context.Context) (Dataset, error) {
	cs, err := r.Store.AtlasCountries(ctx)
	if err != nil {
		return Dataset{}, err
	}
	if len(cs) == 0 {
		return Dataset{}, errors.New("history: no countries")
	}
	ds := Dataset{GeneratedAt: r.Now().UTC().Format(time.RFC3339), Rows: make(map[string]Row, len(cs))}
	var failed []string
	first := true
	for _, c := range cs {
		var row Row
		for _, m := range []struct {
			id     string
			source string
			step   int
			dst    *fred.Measure
		}{
			{fred.SharesID(c.Code), "OECD via FRED " + fred.SharesID(c.Code), 1, &row.Shares},
			{fred.HousingID(c.Code), "BIS via FRED " + fred.HousingID(c.Code), 3, &row.Housing},
		} {
			if !first {
				if err := r.Fred.Sleep(ctx, r.Fred.Gap); err != nil {
					return Dataset{}, err
				}
			}
			first = false
			obs, err := r.Fred.Fetch(ctx, m.id)
			if err == nil {
				s, serr := fred.ToHistory(obs, m.step)
				if serr == nil {
					*m.dst = fred.Measure{Source: m.source, AsOf: s.AsOf(), Series: s}
				}
				err = serr
			}
			if err != nil {
				if ctx.Err() != nil {
					return Dataset{}, ctx.Err()
				}
				r.Log.Warn("history: series failed", "id", m.id, "error", err)
				failed = append(failed, m.id)
			}
		}
		ds.Rows[c.Code] = row
	}
	if len(failed) > 0 {
		return Dataset{}, fmt.Errorf("history: %d series failed: %s", len(failed), strings.Join(failed, ","))
	}

	st, err := r.ECB.Build(ctx)
	if err != nil {
		return Dataset{}, err
	}
	ecbDS := st.ComputeHistory(r.Now())
	ds.Currencies = ecbDS.Currencies
	if _, ok := ecbDS.Currencies[ecb.ChileCode]; ok {
		obs, err := r.Fred.Fetch(ctx, ecb.PesosID)
		if err != nil {
			return Dataset{}, err
		}
		pesos, err := fred.ToHistory(obs, 1)
		if err != nil {
			return Dataset{}, err
		}
		if usd := ecbDS.Currencies["USD"]; usd.Series != nil {
			pesos = ecb.TrimTo(pesos, *usd.Series)
		}
		if err := ecb.DeriveChile(&ecbDS, ecb.ChileCode, pesos); err != nil {
			return Dataset{}, err
		}
		ds.Currencies = ecbDS.Currencies
	}
	return ds, nil
}

// Refresh builds the dataset and, only if that succeeded, stores it.
func (r *Refresher) Refresh(ctx context.Context) error {
	ds, err := r.Build(ctx)
	if err != nil {
		return err
	}
	if err := r.Store.Put(ctx, DatasetName, ds); err != nil {
		return err
	}
	r.Log.Info("history: refreshed", "countries", len(ds.Rows), "currencies", len(ds.Currencies))
	return nil
}

// Stale reports whether the stored dataset is missing or older than maxAge; the
// embedded fallback counts as missing.
func (r *Refresher) Stale(ctx context.Context, maxAge time.Duration) bool {
	return reference.Stale(ctx, r.Store, DatasetName, r.Now(), maxAge, func(d Dataset) string { return d.GeneratedAt })
}
