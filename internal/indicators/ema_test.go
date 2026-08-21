package indicators

import (
	"math"
	"testing"

	"github.com/AkashJam/ticker/internal/source"
)

func closesOf(values ...float64) []source.Candle {
	candles := make([]source.Candle, len(values))
	for i, v := range values {
		candles[i] = source.Candle{Close: v}
	}
	return candles
}

func TestEMA(t *testing.T) {
	// Hand-computed, period=3: seed = SMA(10,11,12) = 11, k = 2/(3+1) = 0.5.
	//   ema = 13*0.5 + 11*0.5 = 12
	//   ema = 14*0.5 + 12*0.5 = 13
	//   ema = 15*0.5 + 13*0.5 = 14
	// All inputs and k are exactly representable in float64, so this
	// compares exactly rather than within a tolerance.
	candles := closesOf(10, 11, 12, 13, 14, 15)

	got, ok := EMA(candles, 3)
	if !ok {
		t.Fatalf("EMA: expected ok=true")
	}
	if got != 14 {
		t.Errorf("EMA = %v, want 14", got)
	}
}

func TestEMA_NotEnoughCandles(t *testing.T) {
	candles := closesOf(10, 11)
	if _, ok := EMA(candles, 3); ok {
		t.Errorf("EMA: expected ok=false with fewer candles than the period")
	}
}

func TestEMA_InvalidPeriod(t *testing.T) {
	if _, ok := EMA(closesOf(10, 11, 12), 0); ok {
		t.Errorf("EMA: expected ok=false for a zero period")
	}
}

func fuzzyEqual(a, b, epsilon float64) bool {
	return math.Abs(a-b) < epsilon
}
