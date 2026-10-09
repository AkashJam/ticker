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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/store"
)

const heartbeatInterval = 15 * time.Second

// Measured latency (phase7.md Step 6) — what the Market
// Ticker case study's tiles read, via the portfolio's server-side
// Prometheus query, instead of the figures it used to estimate.
var (
	// ingestToFanout is tick stamp → SSE write, observed once per viewer
	// write: normalize, XADD, PUBLISH, Redis pub/sub and this hub, as each
	// viewer's stream actually experiences it. Only quotes — candles are
	// aggregator output, not a tick's path. Buckets sized for one box:
	// everything is local, so sub-millisecond to a second covers it.
	ingestToFanout = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ticker_ingest_to_fanout_seconds",
		Help:    "Seconds from a tick's source timestamp to its SSE write to a viewer, by source.",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"source"})

	// sseClients counts admitted stream connections (not ones rejected at
	// the cap). Its max over a window is what "peak concurrent viewers"
	// can honestly claim — an observation, not a capacity number.
	sseClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ticker_sse_clients",
		Help: "SSE stream connections currently open.",
	})
)

// fanoutLatency reads a quote payload's source timestamp and returns the
// seconds elapsed at `now`, plus the source label. ok is false for a
// payload that isn't a tick or carries no timestamp — nothing to observe.
// Pure so it can be tested without a Redis subscription or an HTTP writer.
func fanoutLatency(payload []byte, now time.Time) (seconds float64, sourceLabel string, ok bool) {
	var tick source.NormalizedTick
	if err := json.Unmarshal(payload, &tick); err != nil || tick.Timestamp.IsZero() {
		return 0, "", false
	}
	sourceLabel = "live"
	if tick.Simulated {
		sourceLabel = "sim"
	}
	return now.Sub(tick.Timestamp).Seconds(), sourceLabel, true
}

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
	sseClients.Inc()
	defer sseClients.Dec()

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

	if msg.Type == "quote" {
		if seconds, sourceLabel, ok := fanoutLatency(msg.Payload, time.Now()); ok {
			ingestToFanout.WithLabelValues(sourceLabel).Observe(seconds)
		}
	}
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
