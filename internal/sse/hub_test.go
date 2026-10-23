package sse

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/source"
)

func TestFanoutLatency(t *testing.T) {
	emitted := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	now := emitted.Add(4 * time.Millisecond)

	msg := func(symbol string, at time.Time) source.PubSubMessage {
		// The quote's own time is the bar's first second, seconds before the
		// emit: it must play no part in the span.
		b, err := json.Marshal(source.QuoteEvent{Symbol: symbol, Price: 1, Timestamp: emitted.Add(-2500 * time.Millisecond)})
		if err != nil {
			t.Fatal(err)
		}
		return source.PubSubMessage{Type: "quote", Payload: b, EmittedAt: at}
	}

	t.Run("sim quote", func(t *testing.T) {
		seconds, label, ok := fanoutLatency(msg("SIM:NOVA", emitted), now)
		if !ok || label != "sim" || seconds != 0.004 {
			t.Fatalf("got (%v, %q, %v), want (0.004, \"sim\", true)", seconds, label, ok)
		}
	})

	t.Run("live quote, timed from the emit and not the bar's stamp", func(t *testing.T) {
		seconds, label, ok := fanoutLatency(msg("RY", emitted), now)
		if !ok || label != "live" || seconds != 0.004 {
			t.Fatalf("got (%v, %q, %v), want (0.004, \"live\", true)", seconds, label, ok)
		}
	})

	t.Run("bad json", func(t *testing.T) {
		if _, _, ok := fanoutLatency(source.PubSubMessage{Type: "quote", Payload: []byte("{not json"), EmittedAt: emitted}, now); ok {
			t.Fatal("want ok=false for unparseable payload")
		}
	})

	t.Run("no emit stamp, as from an older publisher during a deploy", func(t *testing.T) {
		if _, _, ok := fanoutLatency(msg("RY", time.Time{}), now); ok {
			t.Fatal("want ok=false when the envelope carries no emit time")
		}
	})
}
