package server

import (
	"testing"

	"github.com/AkashJam/ticker/internal/reference"
)

func TestThinSensors(t *testing.T) {
	thin, other := "thin", "deep"
	got := ThinSensors([]reference.Sensor{
		{Symbol: "KB", Liquidity: &thin},
		{Symbol: "RY", Liquidity: &other},
		{Symbol: "NVDA"}, // a global sensor has no liquidity tier
	})
	if len(got) != 1 || !got["KB"] {
		t.Errorf("thin = %v, want only KB", got)
	}
}
