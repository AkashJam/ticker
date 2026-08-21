// Package sse implements the server side of Option B (portfolio.md §8,
// §9.2, §13): the Go API has no public route, so this Hub is only ever
// reached over the Docker network by the Next.js proxy — never directly by
// a browser. It relays Redis pub/sub (quotes:{symbol}) as Server-Sent
// Events, with a connection cap and a 15s heartbeat.
package sse

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/store"
)

const heartbeatInterval = 15 * time.Second

// Hub fans Redis pub/sub messages out to connected clients as SSE. A plain
// net/http handler (not framework-specific) so it stays reusable regardless
// of what mounts it.
type Hub struct {
	redis    *store.Redis
	maxConns int64
	active   atomic.Int64
	log      *slog.Logger
}

func NewHub(redis *store.Redis, maxConns int64, log *slog.Logger) *Hub {
	return &Hub{redis: redis, maxConns: maxConns, log: log}
}

// ServeHTTP handles one client's `?symbols=SIM:NOVA,SIM:HELIX` subscription
// for the connection's lifetime, per §9.2's sequence: subscribe on Redis,
// relay quote/candle messages as they arrive, heartbeat every 15s, and
// clean up on disconnect (ctx cancellation).
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	symbolsParam := r.URL.Query().Get("symbols")
	if symbolsParam == "" {
		http.Error(w, "symbols query parameter is required", http.StatusBadRequest)
		return
	}

	// Reject-new at the cap (resolved during planning) — simpler than
	// evicting an existing viewer's stream to admit a stranger (§13).
	if h.active.Add(1) > h.maxConns {
		h.active.Add(-1)
		http.Error(w, "too many concurrent stream connections", http.StatusServiceUnavailable)
		return
	}
	defer h.active.Add(-1)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	channels := make([]string, 0, len(symbolsParam))
	for _, sym := range strings.Split(symbolsParam, ",") {
		if sym = strings.TrimSpace(sym); sym != "" {
			channels = append(channels, store.QuoteChannel(sym))
		}
	}

	ctx := r.Context()
	pubsub := h.redis.Subscribe(ctx, channels...)
	defer func() { _ = pubsub.Close() }()

	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()

	msgs := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			h.relay(w, flusher, msg.Payload)
		case <-heartbeat.C:
			h.sendHeartbeat(w, flusher)
		}
	}
}

// relay unwraps the source.PubSubMessage envelope and re-emits it as the
// SSE event name §8 documents (event: quote / event: candle).
func (h *Hub) relay(w http.ResponseWriter, flusher http.Flusher, raw string) {
	var msg source.PubSubMessage
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		h.log.Error("sse: unmarshal envelope", "error", err)
		return
	}
	// A write failure here just means the client disconnected mid-message —
	// the next loop iteration's ctx.Done() check handles cleanup, so this
	// is intentionally not treated as an error to log.
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", msg.Type, msg.Payload)
	flusher.Flush()
}

func (h *Hub) sendHeartbeat(w http.ResponseWriter, flusher http.Flusher) {
	payload, err := json.Marshal(struct {
		Time time.Time `json:"time"`
	}{time.Now().UTC()})
	if err != nil {
		return
	}
	_, _ = fmt.Fprintf(w, "event: heartbeat\ndata: %s\n\n", payload)
	flusher.Flush()
}
