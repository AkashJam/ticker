package indicators

import "github.com/AkashJam/ticker/internal/source"

// SeriesFunc is the extension point (§6.4): every entry is a pure
// func([]Candle) → series, added to Registry — new indicators (§16.1's
// SMA/MACD/Bollinger/VWAP) are new registry entries, not new code paths.
// The returned series is one value per candle from the indicator's seed
// point onward, oldest first.
type SeriesFunc func(candles []source.Candle) (series []float64, ok bool)

// Entry pairs a registry name with its default period and series function.
type Entry struct {
	Period int
	Series SeriesFunc
}

// Registry maps the API's `set=` query values (§8) to their indicator.
var Registry = map[string]Entry{
	"ema": {
		Period: DefaultEMAPeriod,
		Series: func(c []source.Candle) ([]float64, bool) { return EMASeries(c, DefaultEMAPeriod) },
	},
	"rsi": {
		Period: RSIPeriod,
		Series: func(c []source.Candle) ([]float64, bool) { return RSISeries(c, RSIPeriod) },
	},
}
