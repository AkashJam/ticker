package fred

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/AkashJam/ticker/internal/change"
	"github.com/AkashJam/ticker/internal/reference"
)

// DatasetName is the reference-store key and embedded file for this fetcher.
const DatasetName = "fred"

// Measure is one index for one country as a stored series. The route turns it
// into change over a window and per year at the common end (internal/change);
// no level is ever served.
type Measure struct {
	Source string        `json:"source"`
	AsOf   string        `json:"asOf"`
	Series change.Series `json:"series"`
}

type Row struct {
	Shares  Measure `json:"shares"`
	Housing Measure `json:"housing"`
}

// Dataset is the blob stored as "fred". Field names are finalised with the Zod
// schemas in step 5.
type Dataset struct {
	GeneratedAt string         `json:"generatedAt"`
	Rows        map[string]Row `json:"rows"`
}

// SharesID and HousingID are the FRED series for an ISO 3166 alpha-2 code.
func SharesID(code string) string  { return "SPASTT01" + code + "M661N" }
func HousingID(code string) string { return "Q" + code + "R628BIS" }

// SeriesSource is the one call the refresher needs, so tests can fake FRED.
type SeriesSource interface {
	Series(ctx context.Context, id string) ([]Obs, error)
}

// Refresher builds the dataset from FRED and stores it.
type Refresher struct {
	Source   SeriesSource
	Store    *reference.Store
	Gap      time.Duration // between series, to stay polite to a free endpoint
	Attempts int           // per series
	Backoff  time.Duration // before a retry, when the server names no wait
	Now      func() time.Time
	Sleep    func(ctx context.Context, d time.Duration) error
	Log      *slog.Logger
}

func NewRefresher(src SeriesSource, st *reference.Store, log *slog.Logger) *Refresher {
	return &Refresher{
		Source: src, Store: st,
		Gap: 500 * time.Millisecond, Attempts: 3, Backoff: 3 * time.Second,
		Now: time.Now, Sleep: sleepCtx, Log: log,
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Build fetches every series and builds the dataset. It is all or nothing: a
// partial blob would leave countries on different ends, so one failed series
// fails the build and the caller keeps whatever it already had.
func (r *Refresher) Build(ctx context.Context) (Dataset, error) {
	cs, err := r.Store.AtlasCountries(ctx)
	if err != nil {
		return Dataset{}, err
	}
	if len(cs) == 0 {
		return Dataset{}, errors.New("fred: no countries")
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
			dst    *Measure
		}{
			{SharesID(c.Code), "OECD via FRED " + SharesID(c.Code), 1, &row.Shares},
			{HousingID(c.Code), "BIS via FRED " + HousingID(c.Code), 3, &row.Housing},
		} {
			if !first {
				if err := r.Sleep(ctx, r.Gap); err != nil {
					return Dataset{}, err
				}
			}
			first = false
			obs, err := r.fetch(ctx, m.id)
			if err == nil {
				var s change.Series
				if s, err = ToSeries(obs, m.step); err == nil {
					*m.dst = Measure{Source: m.source, AsOf: s.AsOf(), Series: s}
				}
			}
			if err != nil {
				if ctx.Err() != nil {
					return Dataset{}, ctx.Err()
				}
				r.Log.Warn("fred: series failed", "id", m.id, "error", err)
				failed = append(failed, m.id)
			}
		}
		ds.Rows[c.Code] = row
	}
	if len(failed) > 0 {
		return Dataset{}, fmt.Errorf("fred: %d series failed: %s", len(failed), strings.Join(failed, ","))
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
	r.Log.Info("fred: refreshed", "countries", len(ds.Rows))
	return nil
}

// Stale reports whether the stored dataset is missing or older than maxAge,
// so a start-up catch-up fetches only when it has to. The embedded fallback
// counts as missing: it is a floor, not a refresh.
func (r *Refresher) Stale(ctx context.Context, maxAge time.Duration) bool {
	var ds Dataset
	src, err := r.Store.Get(ctx, DatasetName, &ds)
	if err != nil || src != reference.SourceLive {
		return true
	}
	at, err := time.Parse(time.RFC3339, ds.GeneratedAt)
	return err != nil || r.Now().Sub(at) > maxAge
}

// fetch retries what can succeed on a retry.
func (r *Refresher) fetch(ctx context.Context, id string) ([]Obs, error) {
	var lastErr error
	for attempt := 1; attempt <= r.Attempts; attempt++ {
		obs, err := r.Source.Series(ctx, id)
		if err == nil {
			return obs, nil
		}
		lastErr = err
		if ctx.Err() != nil || attempt == r.Attempts {
			break
		}
		var se *StatusError
		wait := r.Backoff
		switch {
		case errors.As(err, &se):
			if !se.Retryable() {
				return nil, err
			}
			if se.RetryAfter > wait {
				wait = se.RetryAfter
			}
		case errors.Is(err, ErrNoData):
			return nil, err
		}
		if err := r.Sleep(ctx, wait); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}
