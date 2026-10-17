package coverage

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/AkashJam/ticker/internal/centralbank"
	"github.com/AkashJam/ticker/internal/edgar"
	"github.com/AkashJam/ticker/internal/news"
	"github.com/AkashJam/ticker/internal/reference"
)

// ReferenceConfig reads sensor configuration from the reference tables: the
// phrases and feed from the country table, the topic from the sensor table, the
// CIK from the edgar dataset, and the excluded outlets from the outlets table.
type ReferenceConfig struct{ Store *reference.Store }

// Sensor implements Config.
func (r ReferenceConfig) Sensor(ctx context.Context, symbol string) (SensorConfig, error) {
	cfg := SensorConfig{Symbol: symbol}

	sensors, err := r.Store.Sensors(ctx)
	if err != nil {
		return cfg, err
	}
	var found bool
	for _, s := range sensors {
		if s.Symbol != symbol {
			continue
		}
		found = true
		cfg.Global = s.Group == "global"
		if s.NewsTopic != nil {
			cfg.Topic = *s.NewsTopic
		}
	}
	if !found {
		return cfg, fmt.Errorf("%s is not a sensor", symbol)
	}

	if !cfg.Global {
		countries, err := r.Store.Countries(ctx)
		if err != nil {
			return cfg, err
		}
		for _, c := range countries {
			if c.Sensor == nil || *c.Sensor != symbol {
				continue
			}
			if c.EconomyQuery != nil {
				cfg.EconomyQuery = *c.EconomyQuery
			}
			if c.SectorQuery != nil {
				cfg.SectorQuery = *c.SectorQuery
			}
			cfg.Feed = c.CentralBankFeed
			if c.CentralBankFeedReason != nil {
				cfg.FeedReason = *c.CentralBankFeedReason
			}
		}
	}

	var ds edgar.Dataset
	if _, err := r.Store.Get(ctx, edgar.DatasetName, &ds); err == nil {
		if e, ok := ds.Sensors[symbol]; ok {
			if n, err := strconv.Atoi(e.CIK); err == nil {
				cfg.CIK = n
			}
		}
	}
	return cfg, nil
}

// OutletsName is the reference dataset of machine-generated outlets.
const OutletsName = "outlets"

// Excluded implements Config.
func (r ReferenceConfig) Excluded(ctx context.Context) (news.ExcludedList, error) {
	var l news.ExcludedList
	if _, err := r.Store.Get(ctx, OutletsName, &l); err != nil {
		return l, err
	}
	return l, nil
}

// NewProduction builds the collector the server and the CLI use: the reference
// tables for configuration, Google News, the central bank feeds, and EDGAR when
// a contact was given (filer may be nil: the filings source is then an error
// frozen at once, never a request that would identify no one).
func NewProduction(store *reference.Store, filer FilingSource, log *slog.Logger) *Collector {
	return NewCollector(ReferenceConfig{Store: store}, news.NewClient(), centralbank.NewClient(), filer, log)
}
