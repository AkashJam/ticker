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
	Liquidity *string `json:"liquidity"` // "thin" for the four thin ADR listings
	// Order is the fixed display order, 1 to 18. It is a field and not derived
	// from region: Türkiye sits between the Netherlands and India, and Canada
	// comes before Mexico (bearing.md #102).
	Order int `json:"order"`
}

type sensorRow struct {
	Symbol string  `json:"symbol"`
	Name   string  `json:"name"`
	Group  string  `json:"group"`
	Theme  *string `json:"theme"`
	Order  int     `json:"order"`
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
		sn := Sensor{Symbol: r.Symbol, Name: r.Name, Group: r.Group, Theme: r.Theme, Order: r.Order}
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
