package ingest

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/source"
)

type fakeLeader struct{ leader bool }

func (f *fakeLeader) IsLeader() bool { return f.leader }

type fakeProducerRedis struct {
	xaddCalls    int
	publishCalls int
	cacheCalls   int
	xaddData     any    // the stream entry's "data"
	published    []byte // the SSE envelope
	cached       string
}

func (f *fakeProducerRedis) XAdd(_ context.Context, _ string, values map[string]any) error {
	f.xaddCalls++
	f.xaddData = values["data"]
	return nil
}

func (f *fakeProducerRedis) Publish(_ context.Context, _ string, payload []byte) error {
	f.publishCalls++
	f.published = payload
	return nil
}

func (f *fakeProducerRedis) CacheSet(_ context.Context, _, value string, _ time.Duration) error {
	f.cacheCalls++
	f.cached = value
	return nil
}

func newTestProducer(leader bool) (*Producer, *fakeProducerRedis) {
	redis := &fakeProducerRedis{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewProducer(redis, &fakeLeader{leader: leader}, log), redis
}

// TestProducer_StandbyDoesNotIngest is the "dedup" property (§6.1): with
// more than one ticker replica running, only the leader ingests — a
// standby must produce nothing at all, so replicas never double-write the
// same ticks onto the Stream.
func TestProducer_StandbyDoesNotIngest(t *testing.T) {
	p, redis := newTestProducer(false)
	p.handle(context.Background(), validTick())

	if redis.xaddCalls != 0 || redis.publishCalls != 0 || redis.cacheCalls != 0 {
		t.Errorf("standby producer should not touch Redis at all, got xadd=%d publish=%d cache=%d",
			redis.xaddCalls, redis.publishCalls, redis.cacheCalls)
	}
}

func TestProducer_LeaderIngests(t *testing.T) {
	p, redis := newTestProducer(true)
	p.handle(context.Background(), validTick())

	if redis.xaddCalls != 1 {
		t.Errorf("expected exactly 1 XAdd call, got %d", redis.xaddCalls)
	}
	if redis.publishCalls != 1 {
		t.Errorf("expected exactly 1 Publish call, got %d", redis.publishCalls)
	}
	if redis.cacheCalls != 1 {
		t.Errorf("expected exactly 1 CacheSet call, got %d", redis.cacheCalls)
	}
}

// The stream entry and the quote cache keep the pipeline's flag; the SSE
// payload readers get must not carry it (ADR-006).
func TestProducer_PublishedQuoteHasNoSimulatedFlag(t *testing.T) {
	p, redis := newTestProducer(true)
	tick := validTick()
	tick.Simulated = true
	p.handle(context.Background(), tick)

	var env source.PubSubMessage
	if err := json.Unmarshal(redis.published, &env); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(env.Payload), "simulated") {
		t.Errorf("SSE payload carries the flag: %s", env.Payload)
	}
	stream, _ := redis.xaddData.([]byte)
	for name, got := range map[string]string{"stream entry": string(stream), "quote cache": redis.cached} {
		if !strings.Contains(got, `"simulated":true`) {
			t.Errorf("%s lost the flag the aggregator needs: %s", name, got)
		}
	}
}

func TestProducer_DropsInvalidTickEvenAsLeader(t *testing.T) {
	p, redis := newTestProducer(true)
	invalid := validTick()
	invalid.Price = -1 // fails Normalize

	p.handle(context.Background(), invalid)

	if redis.xaddCalls != 0 || redis.publishCalls != 0 || redis.cacheCalls != 0 {
		t.Errorf("an invalid tick must never reach Redis, got xadd=%d publish=%d cache=%d",
			redis.xaddCalls, redis.publishCalls, redis.cacheCalls)
	}
}

func TestProducer_Run_StopsOnChannelClose(t *testing.T) {
	p, redis := newTestProducer(true)
	ticks := make(chan source.NormalizedTick, 1)
	ticks <- validTick()
	close(ticks)

	done := make(chan struct{})
	go func() {
		p.Run(context.Background(), ticks)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the tick channel closed")
	}

	if redis.xaddCalls != 1 {
		t.Errorf("expected the buffered tick to be processed before Run returned, xadd=%d", redis.xaddCalls)
	}
}

func TestProducer_LastTick(t *testing.T) {
	p, _ := newTestProducer(false)
	p.handle(context.Background(), validTick())
	if !p.LastTick().IsZero() {
		t.Error("standby should not record a last tick")
	}

	p, _ = newTestProducer(true)
	if !p.LastTick().IsZero() {
		t.Error("LastTick should be zero before any ingest")
	}
	p.handle(context.Background(), validTick())
	if time.Since(p.LastTick()) > time.Second {
		t.Errorf("LastTick should be ~now after leader ingest, got %v", p.LastTick())
	}
}
