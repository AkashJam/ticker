package reference

import "context"

// Country is one row of the country reference table (bearing.md #95): the one
// place the Atlas rows, the sensor reference, the home band and ⌘K read their
// countries from, so the links between the pages cannot drift.
type Country struct {
	Slug        string  `json:"slug"` // the row's address: /atlas#italy
	City        *string `json:"city"` // nil for a country with no Atlas row
	Country     string  `json:"country"`
	Code        string  `json:"code"`     // ISO 3166 alpha-2
	Currency    *string `json:"currency"` // nil for the euro area
	EuroArea    bool    `json:"euroArea"`
	Sensor      *string `json:"sensor"` // market-sensor symbol, if any
	Atlas       bool    `json:"atlas"`  // false for a sensor country with no OECD series
	HasCityPage bool    `json:"hasCityPage"`
	Region      string  `json:"region"`    // americas | europe | asia-pacific | africa | middle-east; geography only, the sensors' display order is sensors.json
	Liquidity   *string `json:"liquidity"` // "thin" for the four thin ADR listings
	NewsTopic   *string `json:"newsTopic"` // Phase 15 fills this
}

// CountriesName is the dataset the table is stored and embedded as.
const CountriesName = "countries"

// Countries returns the whole table, in table order.
func (s *Store) Countries(ctx context.Context) ([]Country, error) {
	var t struct {
		Rows []Country `json:"rows"`
	}
	if _, err := s.Get(ctx, CountriesName, &t); err != nil {
		return nil, err
	}
	return t.Rows, nil
}

// AtlasCountries returns the rows that have an Atlas row: the countries the
// fetchers have series for.
func (s *Store) AtlasCountries(ctx context.Context) ([]Country, error) {
	all, err := s.Countries(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Country, 0, len(all))
	for _, c := range all {
		if c.Atlas {
			out = append(out, c)
		}
	}
	return out, nil
}
