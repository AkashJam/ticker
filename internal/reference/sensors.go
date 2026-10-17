package reference

import (
	"context"
	"fmt"
	"slices"
)

// SensorsName is the dataset the sensor list is stored and embedded as.
const SensorsName = "sensors"

// Sensor is one of the eighteen market sensors as the pages list them: the
// ⌘K palette, the Prices table and the home band's picker all read this, so
// their order and labels cannot drift (bearing.md #95, #102).
type Sensor struct {
	Symbol string `json:"symbol"`
	Name   string `json:"name"`  // the company, short
	Group  string `json:"group"` // country | global
	// Country is the sensor's country, from the country table; nil for a
	// global sensor, which is about a theme rather than a place.
	Country   *string `json:"country"`
	Theme     *string `json:"theme"`     // global sensors only
	Region    *string `json:"region"`    // from the country table; nil for global
	Liquidity *string `json:"liquidity"` // "thin" for the four thin ADR listings, from the country table
	// Note qualifies the company where the table must, e.g. "telecom" or "was
	// Bancolombia".
	Note *string `json:"note"`
	// LiquidityUsdM and LiquidityAsOf are the measured figure behind "thin": the
	// 3-month average daily traded value of the US listing, in $ millions, and
	// the date it was measured (phase8.md, bearing.md #93). Only the four thin
	// sensors carry them.
	LiquidityUsdM *float64 `json:"liquidityUsdM"`
	LiquidityAsOf *string  `json:"liquidityAsOf"`
	// Order is the fixed display order, 1 to 18. It is a field and not derived
	// from region: Türkiye sits between the Netherlands and India, and Canada
	// comes before Mexico (bearing.md #102).
	Order int `json:"order"`

	// NewsTopic is a global sensor's Google News phrase (nil for a country
	// sensor, which uses its country's queries). It is server-side
	// configuration and is not served.
	NewsTopic            *string `json:"-"`
	NewsTopicProvisional bool    `json:"-"`
}

type sensorRow struct {
	Symbol        string   `json:"symbol"`
	Name          string   `json:"name"`
	Group         string   `json:"group"`
	Theme         *string  `json:"theme"`
	Note          *string  `json:"note"`
	LiquidityUsdM *float64 `json:"liquidityUsdM"`
	LiquidityAsOf *string  `json:"liquidityAsOf"`
	Order         int      `json:"order"`

	NewsTopic            *string `json:"newsTopic"`
	NewsTopicProvisional bool    `json:"newsTopicProvisional"`
}

// Sensors returns the eighteen in display order. Country, region and liquidity
// come from the country table at read time; only what the table cannot say
// (the short name, the theme, the order) is stored with the sensor.
func (s *Store) Sensors(ctx context.Context) ([]Sensor, error) {
	var t struct {
		Rows []sensorRow `json:"rows"`
	}
	if _, err := s.Get(ctx, SensorsName, &t); err != nil {
		return nil, err
	}
	countries, err := s.Countries(ctx)
	if err != nil {
		return nil, err
	}
	bySensor := make(map[string]Country, len(countries))
	for _, c := range countries {
		if c.Sensor != nil {
			bySensor[*c.Sensor] = c
		}
	}

	out := make([]Sensor, 0, len(t.Rows))
	for _, r := range t.Rows {
		sn := Sensor{
			Symbol: r.Symbol, Name: r.Name, Group: r.Group, Theme: r.Theme, Note: r.Note,
			LiquidityUsdM: r.LiquidityUsdM, LiquidityAsOf: r.LiquidityAsOf, Order: r.Order,
			NewsTopic: r.NewsTopic, NewsTopicProvisional: r.NewsTopicProvisional,
		}
		if r.Group == "country" {
			c, ok := bySensor[r.Symbol]
			if !ok {
				return nil, fmt.Errorf("reference: sensor %s has no row in the country table", r.Symbol)
			}
			country, region := c.Country, c.Region
			sn.Country, sn.Region, sn.Liquidity = &country, &region, c.Liquidity
		}
		out = append(out, sn)
	}
	slices.SortFunc(out, func(a, b Sensor) int { return a.Order - b.Order })
	return out, nil
}
