package reference

import (
	"context"
	"errors"
	"io/fs"
)

// Country is one row of the country reference table (bearing.md #95): the one
// place the Atlas rows, the sensor reference, the home band and ⌘K read their
// countries from, so the links between the pages cannot drift.
type Country struct {
	Slug     string  `json:"slug"` // the row's address: /atlas#italy
	City     *string `json:"city"` // nil for a country with no Atlas row
	Country  string  `json:"country"`
	Code     string  `json:"code"`     // ISO 3166 alpha-2
	Currency *string `json:"currency"` // nil for the euro area
	EuroArea bool    `json:"euroArea"`
	Sensor   *string `json:"sensor"` // market-sensor symbol, if any
	Atlas    bool    `json:"atlas"`  // false for a sensor country with no OECD series
	// HasCityPage is derived, not stored: a country has a city page when its survey
	// record is verified (Phase 16 step 4). Closing an open item is one edit, in
	// surveys.json, and the page, the /atlas link, the ⌘K entry and the market link follow.
	HasCityPage bool    `json:"-"`
	Region      string  `json:"region"`    // americas | europe | asia-pacific | africa | middle-east; geography only, the sensors' display order is sensors.json
	Liquidity   *string `json:"liquidity"` // "thin" for the four thin ADR listings

	// The next fields are the trigger's news and filing configuration (Phase 15
	// step 5). They exist only on a sensor's row; the rest hold nil.
	EconomyQuery *string `json:"economyQuery"` // Google News phrase for a currency-led trip
	SectorQuery  *string `json:"sectorQuery"`  // Google News phrase for a stock-led trip
	// CentralBankFeed is the country's central bank press feed, or nil with the
	// reason in CentralBankFeedReason.
	CentralBankFeed       *Feed   `json:"centralBankFeed"`
	CentralBankFeedReason *string `json:"centralBankFeedReason"`
	// EdgarCountry is the business country EDGAR gives the sensor's filer, as
	// EDGAR spells it. EdgarOverride is true where that is not the sensor's
	// country (BAP files in Bermuda and senses Peru).
	EdgarCountry  *string `json:"edgarCountry"`
	EdgarOverride bool    `json:"edgarOverride"`
}

// Feed is a central bank press-release feed.
type Feed struct {
	URL      string `json:"url"`
	Format   string `json:"format"`   // rss2 | rss1 | atom
	Language string `json:"language"` // ISO 639-1
	// Timezone names the zone of dates that carry no offset ("Fri, 16 Oct 2026
	// 18:30:00" from the RBI, "Oct 9, 2026, 12:00:00 AM" from the Turkish
	// bank). Empty where the feed's dates carry their own.
	Timezone string `json:"timezone,omitempty"`
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
	surveys, err := s.Surveys(ctx)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err // a store with no surveys file has no city pages, and is not an error
	}
	verified := make(map[string]bool, len(surveys))
	for _, sv := range surveys {
		verified[sv.Slug] = sv.Verified
	}
	for i := range t.Rows {
		t.Rows[i].HasCityPage = t.Rows[i].Atlas && verified[t.Rows[i].Slug]
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
