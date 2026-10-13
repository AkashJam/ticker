package ingest

import (
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/source"
)

func validTick() source.NormalizedTick {
	return source.NormalizedTick{
		Symbol: "SIM:NOVA", Price: 100, Volume: 10,
		Timestamp: time.Now(), AssetType: source.AssetTypeStock,
	}
}

func TestNormalize_Valid(t *testing.T) {
	got, err := Normalize(validTick())
	if err != nil {
		t.Fatalf("Normalize: unexpected error: %v", err)
	}
	if got.Symbol != "SIM:NOVA" || got.Price != 100 {
		t.Errorf("Normalize returned a mutated tick: %+v", got)
	}
}

func TestNormalize_EmptySymbol(t *testing.T) {
	tick := validTick()
	tick.Symbol = ""
	if _, err := Normalize(tick); err == nil {
		t.Errorf("Normalize: expected an error for an empty symbol")
	}
}

func TestNormalize_NonPositivePrice(t *testing.T) {
	for _, price := range []float64{0, -1, -100} {
		tick := validTick()
		tick.Price = price
		if _, err := Normalize(tick); err == nil {
			t.Errorf("Normalize: expected an error for price=%v", price)
		}
	}
}

func TestNormalize_NegativeVolume(t *testing.T) {
	tick := validTick()
	tick.Volume = -1
	if _, err := Normalize(tick); err == nil {
		t.Errorf("Normalize: expected an error for negative volume")
	}
}

func TestNormalize_ZeroTimestamp(t *testing.T) {
	tick := validTick()
	tick.Timestamp = time.Time{}
	if _, err := Normalize(tick); err == nil {
		t.Errorf("Normalize: expected an error for a zero timestamp")
	}
}

func TestNormalize_TimestampTooFarInFuture(t *testing.T) {
	tick := validTick()
	tick.Timestamp = time.Now().Add(1 * time.Hour)
	if _, err := Normalize(tick); err == nil {
		t.Errorf("Normalize: expected an error for a timestamp far in the future")
	}
}

func TestNormalize_TimestampWithinClockSkew(t *testing.T) {
	tick := validTick()
	tick.Timestamp = time.Now().Add(maxClockSkew / 2)
	if _, err := Normalize(tick); err != nil {
		t.Errorf("Normalize: unexpected error for a timestamp within tolerated clock skew: %v", err)
	}
}

func TestNormalize_Bar(t *testing.T) {
	bar := func(open, high, low, close float64) source.NormalizedTick {
		tick := validTick()
		tick.Open, tick.High, tick.Low, tick.Price = open, high, low, close
		return tick
	}
	cases := []struct {
		name    string
		tick    source.NormalizedTick
		wantErr bool
	}{
		{"single price (sim), no bar fields", validTick(), false},
		{"valid bar", bar(100, 103, 99, 101), false},
		{"flat bar", bar(100, 100, 100, 100), false},
		{"partial: high only", func() source.NormalizedTick { t := validTick(); t.High = 101; return t }(), true},
		{"partial: no low", bar(100, 103, 0, 101), true},
		{"high below low", bar(100, 98, 99, 99), true},
		{"open above high", bar(104, 103, 99, 101), true},
		{"close below low", bar(100, 103, 99, 98), true},
		{"negative bar", bar(-1, 103, 99, 101), true},
	}
	for _, c := range cases {
		_, err := Normalize(c.tick)
		if (err != nil) != c.wantErr {
			t.Errorf("%s: error = %v, wantErr %v", c.name, err, c.wantErr)
		}
	}
}
