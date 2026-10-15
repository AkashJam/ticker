package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/source"
)

var (
	tickAt = time.Date(2026, 10, 14, 15, 30, 0, 0, time.UTC)
	barAt  = time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
)

func TestBuildSnapshot_AsOf(t *testing.T) {
	prev := &source.Candle{Close: 100}
	today := &source.Candle{Time: barAt, High: 112, Low: 99, Close: 110}

	t.Run("cached tick dates the price", func(t *testing.T) {
		got := buildSnapshot("RY", &source.NormalizedTick{Price: 105, Timestamp: tickAt}, today, prev)
		if got.AsOf == nil || !got.AsOf.Equal(tickAt) || got.Price != 105 || got.Change != 5 {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("no tick falls back to the latest daily bar", func(t *testing.T) {
		got := buildSnapshot("RY", nil, today, prev)
		if got.AsOf == nil || !got.AsOf.Equal(barAt) || got.Price != 110 {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("nothing at all is a null vintage, not a made-up one", func(t *testing.T) {
		got := buildSnapshot("RY", nil, nil, nil)
		if got.AsOf != nil {
			t.Errorf("asOf = %v, want nil", got.AsOf)
		}
	})
}

// The snapshot is the public contract: it carries a vintage and says nothing
// about whether the source was real (ADR-006).
func TestSnapshotJSONHasAsOfAndNoSimulatedFlag(t *testing.T) {
	for name, snap := range map[string]snapshotResponse{
		"vintage": buildSnapshot("SIM:NOVA", &source.NormalizedTick{Price: 1, Timestamp: tickAt, Simulated: true}, nil, nil),
		"null":    buildSnapshot("RY", nil, nil, nil),
	} {
		b, err := json.Marshal(snap)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		if _, ok := m["asOf"]; !ok {
			t.Errorf("%s: no asOf key in %s", name, b)
		}
		for _, gone := range []string{"simulated", "updatedAt"} {
			if _, ok := m[gone]; ok {
				t.Errorf("%s: %q still in %s", name, gone, b)
			}
		}
	}
}
