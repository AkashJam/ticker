package fred

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/AkashJam/ticker/internal/reference"
)

// DatasetName is the reference-store key and embedded file for this fetcher.
const DatasetName = "fred"

var (
	// Windows are the start years of the three change windows, longest last.
	Windows = []string{"2021", "2016", "2010"}
	// Waypoints are the trail years between the 2010 origin and the end.
	Waypoints = []int{2014, 2018, 2022}
)

// Measure is one index for one country: change over each window, never a level.
type Measure struct {
	Source string              `json:"source"`
	AsOf   string              `json:"asOf"`
	Change map[string]*float64 `json:"change"`
	Trail  []TrailPoint        `json:"trail"`
}

type Row struct {
	Shares  Measure `json:"shares"`
	Housing Measure `json:"housing"`
}

// Dataset is the blob stored as "fred". Field names are finalised with the Zod
// schemas in step 5.
type Dataset struct {
	GeneratedAt string         `json:"generatedAt"`
	Windows     []string       `json:"windows"`
	End         string         `json:"end"` // the latest quarter common to all countries
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

type countries struct {
	Rows []struct {
		Code string `json:"code"`
	} `json:"rows"`
}

type fetched struct{ shares, housing []Obs }

// Build fetches every series and computes the dataset. It is all or nothing:
// the common end needs every country, so one failed series fails the build
// and the caller keeps whatever it already had.
func (r *Refresher) Build(ctx context.Context) (Dataset, error) {
	var cs countries
	if _, err := r.Store.Get(ctx, "countries", &cs); err != nil {
		return Dataset{}, err
	}
	if len(cs.Rows) == 0 {
		return Dataset{}, errors.New("fred: no countries")
	}

	data := make(map[string]fetched, len(cs.Rows))
	var failed []string
	var ends []Quarter
	first := true
	for _, c := range cs.Rows {
		var f fetched
		for _, s := range []struct {
			id  string
			dst *[]Obs
			f   Freq
		}{{SharesID(c.Code), &f.shares, Monthly}, {HousingID(c.Code), &f.housing, Quarterly}} {
			if !first {
				if err := r.Sleep(ctx, r.Gap); err != nil {
					return Dataset{}, err
				}
			}
			first = false
			obs, err := r.fetch(ctx, s.id)
			if err != nil {
				if ctx.Err() != nil {
					return Dataset{}, ctx.Err()
				}
				r.Log.Warn("fred: series failed", "id", s.id, "error", err)
				failed = append(failed, s.id)
				continue
			}
			*s.dst = obs
			ends = append(ends, LastCompleteQuarter(obs, s.f))
		}
		data[c.Code] = f
	}
	if len(failed) > 0 {
		return Dataset{}, fmt.Errorf("fred: %d series failed: %s", len(failed), strings.Join(failed, ","))
	}

	end := CommonEnd(ends)
	ds := Dataset{
		GeneratedAt: r.Now().UTC().Format(time.RFC3339),
		Windows:     Windows,
		End:         end.String(),
		Rows:        make(map[string]Row, len(data)),
	}
	for code, f := range data {
		ds.Rows[code] = Row{
			Shares:  measure("OECD via FRED "+SharesID(code), f.shares, Monthly, end),
			Housing: measure("BIS via FRED "+HousingID(code), f.housing, Quarterly, end),
		}
	}
	return ds, nil
}

func measure(source string, obs []Obs, f Freq, end Quarter) Measure {
	m := Measure{Source: source, AsOf: end.String(), Change: map[string]*float64{}}
	for _, w := range Windows {
		var y int
		_, _ = fmt.Sscanf(w, "%d", &y)
		m.Change[w] = Window(obs, f, y, end)
	}
	m.Trail = Trail(obs, f, Waypoints, end)
	return m
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
	r.Log.Info("fred: refreshed", "end", ds.End, "countries", len(ds.Rows))
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
