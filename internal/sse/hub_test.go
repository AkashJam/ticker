package sse

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/source"
)

func TestFanoutLatency(t *testing.T) {
	stamp := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	now := stamp.Add(4 * time.Millisecond)

	quote := func(simulated bool) []byte {
		b, err := json.Marshal(source.NormalizedTick{Symbol: "SIM:NOVA", Price: 1, Timestamp: stamp, Simulated: simulated})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}

	t.Run("sim quote", func(t *testing.T) {
		seconds, label, ok := fanoutLatency(quote(true), now)
		if !ok || label != "sim" || seconds != 0.004 {
			t.Fatalf("got (%v, %q, %v), want (0.004, \"sim\", true)", seconds, label, ok)
		}
	})

	t.Run("live quote", func(t *testing.T) {
		if _, label, ok := fanoutLatency(quote(false), now); !ok || label != "live" {
			t.Fatalf("got (%q, %v), want (\"live\", true)", label, ok)
		}
	})

	t.Run("bad json", func(t *testing.T) {
		if _, _, ok := fanoutLatency([]byte("{not json"), now); ok {
			t.Fatal("want ok=false for unparseable payload")
		}
	})

	t.Run("zero timestamp", func(t *testing.T) {
		if _, _, ok := fanoutLatency([]byte(`{"symbol":"SIM:NOVA","price":1}`), now); ok {
			t.Fatal("want ok=false when the tick carries no timestamp")
		}
	})
}
