// Package atlas assembles the /atlas comparison from the reference datasets
// (portfolio.md §8). Step 4 of Phase 10 adds the common end year; step 5 adds
// the route's response.
package atlas

import (
	"errors"
	"fmt"

	"github.com/AkashJam/ticker/internal/ecb"
	"github.com/AkashJam/ticker/internal/fred"
	"github.com/AkashJam/ticker/internal/worldbank"
)

// Country is the part of the country table CommonEnd needs: the row's code and
// its currency, nil for the euro area.
type Country struct {
	Code     string
	Currency *string
}

// CommonEnd is the latest year every country has published for all four change
// measures (shares, housing, prices and currency): the end of every window on
// the page (bearing.md #84). Today it is 2024, because the World Bank has no
// US CPI for 2025.
//
// It is computed when the page is served, from the three stored datasets,
// rather than stored by a fetcher: each dataset refreshes on its own schedule,
// and a stored end could disagree with the data beside it.
//
// Euro-area rows have no currency series and are skipped, as is Chile when its
// derived series is stale (bearing.md #85), which is a stated gap and not a
// reason to hold every window back. Any other missing series is an error: a
// window must never be cut at a year one country cannot reach.
func CommonEnd(countries []Country, f fred.Dataset, e ecb.Dataset, w worldbank.Dataset) (int, error) {
	if len(countries) == 0 {
		return 0, errors.New("atlas: no countries")
	}
	end := 0
	note := func(year int) {
		if end == 0 || year < end {
			end = year
		}
	}
	for _, c := range countries {
		fr, ok := f.Rows[c.Code]
		if !ok {
			return 0, fmt.Errorf("atlas: no FRED row for %s", c.Code)
		}
		wr, ok := w.Rows[c.Code]
		if !ok {
			return 0, fmt.Errorf("atlas: no World Bank row for %s", c.Code)
		}
		note(fr.Shares.Series.LastCompleteYear())
		note(fr.Housing.Series.LastCompleteYear())
		note(wr.Prices.Series.LastCompleteYear())

		if c.Currency == nil {
			continue
		}
		cur, ok := e.Currencies[*c.Currency]
		if !ok {
			return 0, fmt.Errorf("atlas: no ECB entry for %s (%s)", *c.Currency, c.Code)
		}
		if cur.Series == nil {
			if cur.Reason == "stale" {
				continue
			}
			return 0, fmt.Errorf("atlas: %s (%s) has no currency series: %s", *c.Currency, c.Code, cur.Reason)
		}
		note(cur.Series.LastCompleteYear())
	}
	return end, nil
}
