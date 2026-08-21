package indicators

import "github.com/AkashJam/ticker/internal/source"

// RSIPeriod is fixed at 14 per portfolio.md §6.4/§8 ("RSI(14)").
const RSIPeriod = 14

// RSI computes the Relative Strength Index over candle closes using
// Wilder's original smoothing method (α = 1/period) — not a plain
// SMA-of-gains/losses RSI, which is a common and easy mistake (flagged
// during planning: portfolio.md §15 itself names an "RSI Wilder-smoothing
// bug" as an anticipated war story). Needs at least period+1 candles to
// produce the first value; ok=false otherwise.
func RSI(candles []source.Candle, period int) (value float64, ok bool) {
	if period <= 0 || len(candles) < period+1 {
		return 0, false
	}

	deltas := make([]float64, len(candles)-1)
	for i := 1; i < len(candles); i++ {
		deltas[i-1] = candles[i].Close - candles[i-1].Close
	}

	// Seed: simple average of the first `period` gains/losses.
	var avgGain, avgLoss float64
	for _, d := range deltas[:period] {
		if d > 0 {
			avgGain += d
		} else {
			avgLoss += -d
		}
	}
	avgGain /= float64(period)
	avgLoss /= float64(period)

	// Wilder smoothing over the remaining deltas: each new average blends
	// in one more period's worth of weight while retaining (period-1)/period
	// of the running average — equivalent to an EMA with α = 1/period.
	for _, d := range deltas[period:] {
		gain, loss := 0.0, 0.0
		if d > 0 {
			gain = d
		} else {
			loss = -d
		}
		avgGain = (avgGain*float64(period-1) + gain) / float64(period)
		avgLoss = (avgLoss*float64(period-1) + loss) / float64(period)
	}

	switch {
	case avgLoss == 0 && avgGain == 0:
		return 50, true // no movement at all in the window
	case avgLoss == 0:
		return 100, true // only gains — RS is unbounded, RSI saturates at 100
	default:
		rs := avgGain / avgLoss
		return 100 - (100 / (1 + rs)), true
	}
}
