package ecb

import (
	"context"
	"fmt"
	"time"

	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/trigger"
)

// LocalPerUSD returns a currency's units per US dollar by calendar date, from
// the state's carried-forward daily windows: (units per €1) / (dollars per €1).
// The euro itself is 1 / (dollars per €1). A date appears only where both
// legs have a rate. It is nil for a currency the state holds no window for.
func (s *State) LocalPerUSD(code string) map[time.Time]float64 {
	usd := s.Window["USD"]
	if usd == nil {
		return nil
	}
	if code == "EUR" {
		out := make(map[time.Time]float64, len(usd.Rates))
		for i, r := range usd.Rates {
			if r > 0 {
				out[usd.Start.AddDate(0, 0, i)] = 1 / r
			}
		}
		return out
	}
	w := s.Window[code]
	if w == nil {
		return nil
	}
	byDate := make(map[time.Time]float64, len(usd.Rates))
	for i, r := range usd.Rates {
		byDate[usd.Start.AddDate(0, 0, i)] = r
	}
	out := make(map[time.Time]float64, len(w.Rates))
	for i, r := range w.Rates {
		d := w.Start.AddDate(0, 0, i)
		if u := byDate[d]; u > 0 && r > 0 {
			out[d] = r / u
		}
	}
	return out
}

// RateSource gives the trigger a sensor's home-currency rates (phase15.md
// step 4). The history comes from the stored ECB state, which the daily job
// advances at 06:00 UTC; the session's own rate, published about 16:00 CET and
// so not yet in the state when the sweep runs, is fetched from the daily file,
// and only when a trip needs it.
type RateSource struct {
	Store *reference.Store
	Feed  Feed
}

// Rates implements trigger.RateSource.
func (r *RateSource) Rates(ctx context.Context, symbol string, session time.Time) (trigger.RateSet, error) {
	code, err := r.currencyOf(ctx, symbol)
	if err != nil {
		return trigger.RateSet{}, err
	}

	var st State
	src, err := r.Store.Get(ctx, StateName, &st)
	if err != nil {
		return trigger.RateSet{}, fmt.Errorf("ecb: read state: %w", err)
	}
	if src != reference.SourceLive {
		// The state exists only in the cache; the embedded fallback holds the
		// monthly dataset and cannot give a daily window.
		return trigger.RateSet{}, fmt.Errorf("ecb: no stored state (cache %s)", src)
	}
	if code != "EUR" && st.Window[code] == nil {
		if code == ChileCode {
			return trigger.RateSet{}, &trigger.NoSplitError{Reason: trigger.NoSplitMonthlyOnly}
		}
		return trigger.RateSet{}, &trigger.NoSplitError{Reason: trigger.NoSplitNoECBCurrency}
	}

	source := "state"
	if st.Last.Before(session) {
		day, err := r.Feed.Daily(ctx)
		if err != nil {
			return trigger.RateSet{}, fmt.Errorf("ecb: daily file: %w", err)
		}
		switch {
		case day.Date.Before(session):
			// The ECB has not published the session's fix: a TARGET holiday, or
			// not yet. Final: a later run would not know it either.
			return trigger.RateSet{}, &trigger.NoSplitError{Reason: trigger.NoSplitNotPublished}
		case day.Date.After(session):
			return trigger.RateSet{}, fmt.Errorf("ecb: the stored state ends %s, before the session %s, and the daily file is already %s",
				st.Last.Format(time.DateOnly), session.Format(time.DateOnly), day.Date.Format(time.DateOnly))
		}
		st.Append([]Day{day})
		source = "daily"
	}

	per := st.LocalPerUSD(code)
	if _, ok := per[session]; !ok {
		return trigger.RateSet{}, &trigger.NoSplitError{Reason: trigger.NoSplitNotPublished}
	}
	return trigger.RateSet{Currency: code, PerUSD: per, Source: source, FixDate: session}, nil
}

// currencyOf is the sensor's home currency, from the country table. A sensor
// with no country (a global name) has none; the euro area's is the euro.
func (r *RateSource) currencyOf(ctx context.Context, symbol string) (string, error) {
	countries, err := r.Store.Countries(ctx)
	if err != nil {
		return "", fmt.Errorf("ecb: country table: %w", err)
	}
	for _, c := range countries {
		if c.Sensor == nil || *c.Sensor != symbol {
			continue
		}
		switch {
		case c.EuroArea:
			return "EUR", nil
		case c.Currency != nil:
			return *c.Currency, nil
		}
	}
	return "", &trigger.NoSplitError{Reason: trigger.NoSplitNoHomeCurrency}
}
