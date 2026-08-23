package indicators

import "testing"

func TestRSI_WilderSmoothing(t *testing.T) {
	// Hand-computed, period=3, closes = [10, 12, 11, 13, 12, 14].
	// Deltas: +2, -1, +2, -1, +2.
	//
	// Seed (simple average of the first 3 deltas: +2, -1, +2):
	//   avgGain0 = (2+0+2)/3 = 4/3
	//   avgLoss0 = (0+1+0)/3 = 1/3
	//
	// Wilder smoothing over the remaining 2 deltas (-1, then +2) — this is
	// the step a plain SMA-based RSI gets wrong (portfolio.md §15 names
	// exactly this class of bug):
	//   delta -1: avgGain1 = (4/3*2 + 0)/3 = 8/9;  avgLoss1 = (1/3*2 + 1)/3 = 5/9
	//   delta +2: avgGain2 = (8/9*2 + 2)/3 = 34/27; avgLoss2 = (5/9*2 + 0)/3 = 10/27
	//
	// RS = (34/27)/(10/27) = 3.4
	// RSI = 100 - 100/(1+3.4) = 100 - 250/11 = 850/11 ≈ 77.2727...
	candles := closesOf(10, 12, 11, 13, 12, 14)
	want := 850.0 / 11.0

	got, ok := RSI(candles, 3)
	if !ok {
		t.Fatalf("RSI: expected ok=true")
	}
	if !fuzzyEqual(got, want, 1e-9) {
		t.Errorf("RSI = %v, want %v", got, want)
	}
}

func TestRSISeries_WilderSmoothing(t *testing.T) {
	// Same input/worked example as TestRSI_WilderSmoothing, but checking
	// every intermediate point, not just the final value — this is exactly
	// where a plain-SMA RSI bug would diverge from Wilder's smoothing but a
	// final-value-only test could still coincidentally pass on other inputs.
	//   series[0] (seed):    avgGain=4/3, avgLoss=1/3 → RSI = 100-100/5    = 80
	//   series[1] (delta -1): avgGain=8/9, avgLoss=5/9 → RSI = 100-100/2.6  = 800/13
	//   series[2] (delta +2): avgGain=34/27, avgLoss=10/27 → RSI = 850/11 (final)
	candles := closesOf(10, 12, 11, 13, 12, 14)
	want := []float64{80, 800.0 / 13.0, 850.0 / 11.0}

	got, ok := RSISeries(candles, 3)
	if !ok {
		t.Fatalf("RSISeries: expected ok=true")
	}
	if len(got) != len(want) {
		t.Fatalf("RSISeries: got %d points, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if !fuzzyEqual(got[i], want[i], 1e-9) {
			t.Errorf("RSISeries[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestRSISeries_NotEnoughCandles(t *testing.T) {
	candles := closesOf(10, 11, 12)
	if _, ok := RSISeries(candles, 3); ok {
		t.Errorf("RSISeries: expected ok=false with only period candles (need period+1)")
	}
}

func TestRSI_AllGains(t *testing.T) {
	// Strictly increasing closes ⇒ avgLoss stays 0 ⇒ RSI saturates at 100.
	candles := closesOf(10, 11, 12, 13, 14)
	got, ok := RSI(candles, 3)
	if !ok {
		t.Fatalf("RSI: expected ok=true")
	}
	if got != 100 {
		t.Errorf("RSI = %v, want 100", got)
	}
}

func TestRSI_AllLosses(t *testing.T) {
	// Strictly decreasing closes ⇒ avgGain stays 0 ⇒ RSI is 0.
	candles := closesOf(14, 13, 12, 11, 10)
	got, ok := RSI(candles, 3)
	if !ok {
		t.Fatalf("RSI: expected ok=true")
	}
	if got != 0 {
		t.Errorf("RSI = %v, want 0", got)
	}
}

func TestRSI_NotEnoughCandles(t *testing.T) {
	// RSI(period) needs period+1 candles (period deltas); period+0 isn't enough.
	candles := closesOf(10, 11, 12)
	if _, ok := RSI(candles, 3); ok {
		t.Errorf("RSI: expected ok=false with only period candles (need period+1)")
	}
}

func TestRSI_InvalidPeriod(t *testing.T) {
	if _, ok := RSI(closesOf(10, 11, 12), 0); ok {
		t.Errorf("RSI: expected ok=false for a zero period")
	}
}
