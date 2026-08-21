package indicators

import "github.com/AkashJam/ticker/internal/source"

// ComputeFunc is the extension point (§6.4): every entry is a pure
// func([]Candle) → value, added to Registry — new indicators (§16.1's
// SMA/MACD/Bollinger/VWAP) are new registry entries, not new code paths.
type ComputeFunc func(candles []source.Candle) (value float64, ok bool)

// Entry pairs a registry name with its default period and compute
// function.
type Entry struct {
	Period  int
	Compute ComputeFunc
}

// Registry maps the API's `set=` query values (§8) to their indicator.
var Registry = map[string]Entry{
	"ema": {
		Period:  DefaultEMAPeriod,
		Compute: func(c []source.Candle) (float64, bool) { return EMA(c, DefaultEMAPeriod) },
	},
	"rsi": {
		Period:  RSIPeriod,
		Compute: func(c []source.Candle) (float64, bool) { return RSI(c, RSIPeriod) },
	},
}
