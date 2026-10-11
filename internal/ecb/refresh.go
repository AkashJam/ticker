package ecb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/AkashJam/ticker/internal/fred"
	"github.com/AkashJam/ticker/internal/reference"
)

const (
	// DatasetName is the served, embedded blob; StateName is the Redis-only
	// working state behind it.
	DatasetName = "ecb"
	StateName   = "ecb_state"

	// maxGapDays is the longest outage the daily file can bridge. Past it the
	// state is re-seeded from the history zip: carrying one rate across weeks
	// would invent flat days.
	maxGapDays = 5
)

// Feed is the two ECB downloads, so tests can fake them.
type Feed interface {
	History(ctx context.Context) ([]Day, error)
	Daily(ctx context.Context) (Day, error)
}

// PesosID is the OECD series of Chilean pesos per US dollar (monthly average of
// daily rates). The ECB publishes no peso, so Chile is derived through the
// dollar (bearing.md #85).
const PesosID = "CCUSMA02CLM618N"

// ChileCode is the currency derived through the dollar.
const ChileCode = "CLP"

// Pesos is FRED's series call, so tests can fake it; *fred.Client satisfies it.
type Pesos interface {
	Series(ctx context.Context, id string) ([]fred.Obs, error)
}

// Refresher keeps the "ecb" dataset current.
type Refresher struct {
	Feed     Feed
	Pesos    Pesos
	Store    *reference.Store
	Attempts int
	Backoff  time.Duration
	Now      func() time.Time
	Sleep    func(ctx context.Context, d time.Duration) error
	Log      *slog.Logger
}

func NewRefresher(feed Feed, pesos Pesos, st *reference.Store, log *slog.Logger) *Refresher {
	return &Refresher{Feed: feed, Pesos: pesos, Store: st, Attempts: 3, Backoff: 3 * time.Second, Now: time.Now, Sleep: sleepCtx, Log: log}
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

// wanted is every non-euro currency in countries.json, sorted. The euro-area
// rows have no currency of their own to compare.
func (r *Refresher) wanted(ctx context.Context) ([]string, error) {
	var cs struct {
		Rows []struct {
			Currency *string `json:"currency"`
		} `json:"rows"`
	}
	if _, err := r.Store.Get(ctx, "countries", &cs); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []string
	for _, row := range cs.Rows {
		if row.Currency != nil && !seen[*row.Currency] {
			seen[*row.Currency] = true
			out = append(out, *row.Currency)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("ecb: no currencies")
	}
	if !seen["USD"] {
		out = append(out, "USD") // Chile is derived through the dollar
	}
	sort.Strings(out)
	return out, nil
}

// Seed rebuilds the state from the full history zip.
func (r *Refresher) Seed(ctx context.Context, wanted []string) (*State, error) {
	var days []Day
	if err := r.retry(ctx, func() (err error) { days, err = r.Feed.History(ctx); return }); err != nil {
		return nil, err
	}
	st := NewState(wanted)
	st.Append(days)
	return st, nil
}

// Build returns the current state: the stored one advanced by the daily file,
// or a fresh seed from the zip when there is none, the currency set changed,
// or the gap is too long to bridge.
func (r *Refresher) Build(ctx context.Context) (*State, error) {
	wanted, err := r.wanted(ctx)
	if err != nil {
		return nil, err
	}
	var st State
	src, err := r.Store.Get(ctx, StateName, &st)
	if err != nil || src != reference.SourceLive || st.Last.IsZero() || !sameSet(st.Wanted, wanted) {
		r.Log.Info("ecb: seeding from history")
		return r.Seed(ctx, wanted)
	}

	var day Day
	if err := r.retry(ctx, func() (err error) { day, err = r.Feed.Daily(ctx); return }); err != nil {
		return nil, err
	}
	if gap := int(day.Date.Sub(st.Last).Hours() / 24); gap > maxGapDays {
		r.Log.Warn("ecb: gap too long for the daily file, re-seeding", "days", gap)
		return r.Seed(ctx, wanted)
	}
	if day.Date.After(st.Last) {
		st.Append([]Day{day})
	}
	return &st, nil
}

// Compute builds the served dataset from st and derives Chile through the
// dollar. A pesos series that is merely stale is a normal state (Chile is
// served as a stated gap); one that cannot be fetched fails the call.
func (r *Refresher) Compute(ctx context.Context, st *State) (Dataset, error) {
	ds := st.Compute(r.Now())
	if _, wanted := ds.Currencies[ChileCode]; !wanted {
		return ds, nil
	}
	var obs []fred.Obs
	if err := r.retry(ctx, func() (err error) { obs, err = r.Pesos.Series(ctx, PesosID); return }); err != nil {
		return Dataset{}, err
	}
	pesos, err := fred.ToSeries(obs, 1)
	if err != nil {
		return Dataset{}, err
	}
	if err := DeriveChile(&ds, ChileCode, pesos); err != nil {
		return Dataset{}, err
	}
	return ds, nil
}

// Refresh advances the state and stores both blobs, only if nothing failed.
func (r *Refresher) Refresh(ctx context.Context) error {
	st, err := r.Build(ctx)
	if err != nil {
		return err
	}
	ds, err := r.Compute(ctx, st)
	if err != nil {
		return err
	}
	if err := r.Store.Put(ctx, StateName, st); err != nil {
		return err
	}
	if err := r.Store.Put(ctx, DatasetName, ds); err != nil {
		return err
	}
	r.Log.Info("ecb: refreshed", "asOf", ds.AsOf, "currencies", len(ds.Currencies))
	return nil
}

// Stale reports whether the stored dataset is missing or older than maxAge.
// The embedded fallback counts as missing: it is a floor, not a refresh.
func (r *Refresher) Stale(ctx context.Context, maxAge time.Duration) bool {
	var ds Dataset
	src, err := r.Store.Get(ctx, DatasetName, &ds)
	if err != nil || src != reference.SourceLive {
		return true
	}
	at, err := time.Parse(time.RFC3339, ds.GeneratedAt)
	return err != nil || r.Now().Sub(at) > maxAge
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// retry runs fn, retrying what can succeed on a retry.
func (r *Refresher) retry(ctx context.Context, fn func() error) error {
	var lastErr error
	for attempt := 1; attempt <= r.Attempts; attempt++ {
		err := fn()
		if err == nil {
			return nil
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
				return err
			}
			if se.RetryAfter > wait {
				wait = se.RetryAfter
			}
		case errors.Is(err, ErrNoData):
			return err
		}
		if err := r.Sleep(ctx, wait); err != nil {
			return err
		}
	}
	return fmt.Errorf("ecb: giving up: %w", lastErr)
}
