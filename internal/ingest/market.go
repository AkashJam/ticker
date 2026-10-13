package ingest

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	// openRecheck spaces status calls while the market is open and ticks are
	// stale (a genuine outage), so the health check cannot hammer Finnhub.
	openRecheck = 5 * time.Minute
	// errorRetry spaces calls after a failed one, for the same reason.
	errorRetry = time.Minute

	// openHour and openMinute are the regular session's start, in New York.
	// A closed answer is cached until the next weekday's open, the earliest
	// time it can stop being true.
	openHour   = 9
	openMinute = 30
)

var (
	marketStatusCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ticker_market_status_calls_total",
		Help: "Finnhub market-status calls made by the dead-man health check, by result.",
	}, []string{"result"})
	marketClosed = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ticker_market_closed",
		Help: "1 while the health check treats the US market as closed (a cached market-status answer).",
	})
)

// MarketState is what the health check needs from a market-status answer.
type MarketState struct {
	Open    bool
	Session string
	Holiday string
}

// StatusFunc asks whether the market is open now. The gate takes a function
// rather than the Finnhub client, so this package does not depend on sweep.
type StatusFunc func(ctx context.Context) (MarketState, error)

// MarketGate tells the dead-man health check whether silence from the feed is
// expected (portfolio.md §15 Phase 13 step 3). It does not poll. While ticks
// flow it is never consulted; it is asked only when the last tick is already
// stale, and a "closed" answer is cached until the next weekday's open, so a
// night, a weekend or a holiday costs one call. Holidays and early closes come
// from the answer, not from a table.
//
// Errors fail toward alerting: a call that fails is "not known closed", because
// a health check that stays quiet when it cannot find out is the dangerous
// direction.
type MarketGate struct {
	status StatusFunc
	loc    *time.Location
	log    *slog.Logger

	// mu is held across the status call. The dead-man loop calls once a
	// minute from one goroutine, so there is no contention to protect.
	mu          sync.Mutex
	closedUntil time.Time // a closed answer holds until here; zero when none
	floor       time.Time // the most recent cached resume time that has passed
	lastOpenAsk time.Time
	lastErrAt   time.Time
}

// NewMarketGate returns a gate that asks status, with the next open computed in
// loc (America/New_York).
func NewMarketGate(status StatusFunc, loc *time.Location, log *slog.Logger) *MarketGate {
	return &MarketGate{status: status, loc: loc, log: log}
}

// ResumeFloor is the most recent time a cached closed window ended. The health
// check measures tick age from no earlier than this, so the first check after
// 09:30 does not count the whole night's silence against the new session.
func (g *MarketGate) ResumeFloor(now time.Time) time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.rollover(now)
	return g.floor
}

// Closed reports whether the market is closed, asking Finnhub only when no
// cached closed answer covers now and no recent call makes another one pointless.
func (g *MarketGate) Closed(ctx context.Context, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.rollover(now)

	if !g.closedUntil.IsZero() {
		return true
	}
	if !g.lastOpenAsk.IsZero() && now.Sub(g.lastOpenAsk) < openRecheck {
		return false
	}
	if !g.lastErrAt.IsZero() && now.Sub(g.lastErrAt) < errorRetry {
		return false
	}

	st, err := g.status(ctx)
	if err != nil {
		g.lastErrAt = now
		marketStatusCalls.WithLabelValues("error").Inc()
		g.log.Warn("market: status unavailable, treating the market as open", "error", err)
		return false
	}
	g.lastErrAt = time.Time{}
	if st.Open {
		g.lastOpenAsk = now
		marketStatusCalls.WithLabelValues("open").Inc()
		return false
	}

	g.closedUntil = g.nextOpen(now)
	g.lastOpenAsk = time.Time{}
	marketStatusCalls.WithLabelValues("closed").Inc()
	marketClosed.Set(1)
	g.log.Info("market: closed, tick-age check paused", "holiday", st.Holiday, "session", st.Session, "until", g.closedUntil)
	return true
}

// rollover ends a cached closed window once its resume time has passed.
func (g *MarketGate) rollover(now time.Time) {
	if g.closedUntil.IsZero() || now.Before(g.closedUntil) {
		return
	}
	g.floor = g.closedUntil
	g.closedUntil = time.Time{}
	g.lastOpenAsk = time.Time{}
	marketClosed.Set(0)
	g.log.Info("market: closed window ended, tick-age check resumes", "at", g.floor)
}

// nextOpen is the first weekday 09:30 in loc strictly after now. Each
// candidate is built with time.Date in loc, so the clock change on 1 November
// 2026 moves the UTC instant and not the New York time. It knows no holidays: a
// holiday is found out by asking again at that open and caching another answer.
func (g *MarketGate) nextOpen(now time.Time) time.Time {
	local := now.In(g.loc)
	y, m, d := local.Date()
	for i := range 8 {
		c := time.Date(y, m, d+i, openHour, openMinute, 0, 0, g.loc)
		if wd := c.Weekday(); wd == time.Saturday || wd == time.Sunday {
			continue
		}
		if c.After(now) {
			return c
		}
	}
	panic("ingest: no weekday within 8 days") // unreachable
}

// TickFresh is the dead-man check's verdict on the feed: true when the last
// tick is younger than maxAge, or when the silence is expected because the
// market is closed. gate is nil for a feed that never goes quiet (the sim), in
// which case this is plain tick age. A stale feed with a gate asks Finnhub
// once (see Closed) and otherwise makes no call.
//
// Tick age is measured from no earlier than the end of a cached closed window,
// so the first check after the open does not count the night against the new
// session.
func TickFresh(ctx context.Context, now, lastTick time.Time, maxAge time.Duration, gate *MarketGate) bool {
	if gate != nil && gate.ResumeFloor(now).After(lastTick) {
		lastTick = gate.ResumeFloor(now)
	}
	if now.Sub(lastTick) < maxAge {
		return true
	}
	return gate != nil && gate.Closed(ctx, now)
}
