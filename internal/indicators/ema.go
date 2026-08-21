// Package indicators computes technical indicators over candle series
// (portfolio.md §6.4) as a registry of pure functions behind one cache
// wrapper (cache.go) — EMA (this file) and RSI (rsi.go) are v1's two
// entries; SMA/MACD/Bollinger/VWAP (§16.1) are future drop-in additions to
// the same registry, not new architecture.
package indicators

import "github.com/AkashJam/ticker/internal/source"

// DefaultEMAPeriod matches §8's example response (`"ema": {"period": 20`).
const DefaultEMAPeriod = 20

// EMA computes the exponential moving average of candle closes over
// `period`. Returns ok=false if there aren't enough candles to seed it.
func EMA(candles []source.Candle, period int) (value float64, ok bool) {
	if period <= 0 || len(candles) < period {
		return 0, false
	}

	// Seed with a simple average of the first `period` closes, then apply
	// the standard EMA recurrence over the rest — the conventional way to
	// start an EMA series without an arbitrary prior value.
	var sum float64
	for _, c := range candles[:period] {
		sum += c.Close
	}
	ema := sum / float64(period)

	k := 2.0 / float64(period+1)
	for _, c := range candles[period:] {
		ema = c.Close*k + ema*(1-k)
	}

	return ema, true
}
