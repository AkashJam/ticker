package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/AkashJam/ticker/internal/source"
)

var (
	evaluated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ticker_trigger_evaluated_total",
		Help: "Sensor-sessions the threshold job evaluated to a z.",
	})
	skipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ticker_trigger_skipped_total",
		Help: "Sensor-sessions the threshold job could not evaluate, by reason.",
	}, []string{"reason"})
	trips = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ticker_trigger_trips_total",
		Help: "Sensor-sessions whose residual z passed the threshold.",
	})
	splits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ticker_trigger_split_total",
		Help: "Currency splits attempted, by result: a label, none (a final answer with no split), or error (retried next run).",
	}, []string{"result"})
	runErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ticker_trigger_errors_total",
		Help: "Threshold-job runs that failed before recording every trip.",
	})
	mismatches = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ticker_trigger_mismatches_total",
		Help: "Re-evaluated trips whose figures differ from the recorded event (the record is kept).",
	})
)

// EventStore is where events are recorded.
type EventStore interface {
	// InsertEvent records the trip unless one exists for (symbol, session);
	// inserted reports whether this call wrote it.
	InsertEvent(ctx context.Context, t Trip) (inserted bool, err error)
	// Event returns the recorded trip, if any.
	Event(ctx context.Context, symbol string, session time.Time) (Trip, bool, error)
	// SetSplit records the currency split once: it writes only where the
	// event has none, and reports whether it did.
	SetSplit(ctx context.Context, symbol string, session time.Time, label *string, split []byte) (set bool, err error)
	// SetCoverage records an event's coverage, replacing an earlier stage.
	SetCoverage(ctx context.Context, symbol string, session time.Time, coverage []byte) error
	// PendingCoverage returns the recent events before session whose coverage is
	// missing or not complete, oldest first.
	PendingCoverage(ctx context.Context, before time.Time) ([]Trip, error)
}

// The stages of an event's coverage: the fetch at the trip, then the follow-up
// at the next session's run (phase15.md step 6, decision 2).
const (
	StageTrip     = "trip"
	StageFollowUp = "followUp"
)

// RetryGap is how long a run waits before fetching an event's coverage again, so
// a day of restarts cannot use up a source's attempts.
const RetryGap = 6 * time.Hour

// CoverageSource collects an event's news and filings. prev is the coverage
// stored so far (nil at the trip); it returns the coverage to store.
type CoverageSource interface {
	Collect(ctx context.Context, ev Trip, prev []byte, stage string) ([]byte, error)
}

// ThinFunc reports which sensors are tagged thin in the reference table.
type ThinFunc func(ctx context.Context) (map[string]bool, error)

// Runner is the threshold job: it judges one session's closes and records
// every trip.
type Runner struct {
	Candles   RangeReader
	Events    EventStore
	Thin      ThinFunc
	Loc       *time.Location
	Threshold float64
	// Rates and SplitThreshold drive the currency split. With no Rates the
	// split is not attempted and every trip stays pending.
	Rates          RateSource
	SplitThreshold float64
	// Coverage fetches the news and filings for each trip. With none, coverage
	// is not attempted.
	Coverage CoverageSource
	Now      func() time.Time
	Log      *slog.Logger
}

// Summary reports one Run.
type Summary struct {
	Session    time.Time
	Evaluated  int
	Skipped    map[string]string // symbol to reason
	Trips      []Trip
	Inserted   int
	Existing   int // already recorded
	Mismatched []string
	// Splits is the outcome of each split attempted this run, by symbol; a
	// symbol whose attempt failed transiently is absent from it.
	Splits map[string]SplitResult
	// Coverages is the coverage each fetch produced this run, by symbol, whether
	// or not it was stored (a dry run stores nothing).
	Coverages map[string][]byte
}

// ReasonOf names why a sensor could not be evaluated, for the metric and the
// log. The names are a closed set.
func ReasonOf(err error) string {
	var ih *InsufficientHistoryError
	switch {
	case errors.Is(err, ErrNoSession):
		return "no_session"
	case errors.Is(err, ErrNoReturn):
		return "no_return"
	case errors.As(err, &ih):
		return "insufficient_history"
	case errors.Is(err, ErrZeroSigma):
		return "zero_sigma"
	}
	return "error"
}

// Run judges the sensors on session and records every trip. It is idempotent:
// a trip already recorded stays as it was (a re-run that finds different
// figures logs the mismatch and keeps the record). It records nothing if the
// reference lookup fails, because a trip written without its caveats could not
// be corrected afterwards.
func (r *Runner) Run(ctx context.Context, session time.Time) (Summary, error) {
	sum, err := r.run(ctx, session)
	if err != nil {
		runErrors.Inc()
	}
	return sum, err
}

func (r *Runner) run(ctx context.Context, session time.Time) (Summary, error) {
	session = day(session)
	sum := Summary{Session: session, Skipped: map[string]string{}, Splits: map[string]SplitResult{}, Coverages: map[string][]byte{}}

	var sensors []string
	for _, s := range source.StreamUniverse() {
		sensors = append(sensors, s.Symbol)
	}
	symbols := slices.Clone(source.SweepSymbolCodes())

	all, err := LoadAsOf(ctx, r.Candles, symbols, session)
	if err != nil {
		return sum, err
	}
	results, errs := EvaluateOn(all, sensors, session)
	for _, s := range sensors {
		if e, ok := errs[s]; ok {
			reason := ReasonOf(e)
			sum.Skipped[s] = reason
			skipped.WithLabelValues(reason).Inc()
			r.Log.Info("trigger: sensor not evaluated", "symbol", s, "session", session.Format(time.DateOnly), "reason", reason, "error", e)
		}
	}
	sum.Evaluated = len(results)
	evaluated.Add(float64(len(results)))

	var thin map[string]bool
	for _, res := range results {
		if Fire(res.Z, r.Threshold) {
			if thin == nil {
				if thin, err = r.Thin(ctx); err != nil {
					return sum, fmt.Errorf("trigger: reference lookup: %w", err)
				}
			}
			sum.Trips = append(sum.Trips, NewTrip(res, r.Threshold, thin[res.Symbol], r.Loc))
		}
	}
	trips.Add(float64(len(sum.Trips)))

	byResult := map[string]Result{}
	for _, res := range results {
		byResult[res.Symbol] = res
	}
	for _, t := range sum.Trips {
		if err := ValidateCaveats(t.Caveats); err != nil {
			return sum, err
		}
		inserted, err := r.Events.InsertEvent(ctx, t)
		if err != nil {
			return sum, fmt.Errorf("trigger: record %s %s: %w", t.Symbol, session.Format(time.DateOnly), err)
		}
		if inserted {
			sum.Inserted++
			r.Log.Info("trigger: trip", "new", true, "symbol", t.Symbol, "session", session.Format(time.DateOnly), "z", t.Z, "residual", t.Residual, "caveats", t.Caveats)
			r.split(ctx, &sum, all, byResult[t.Symbol])
			continue
		}
		sum.Existing++
		old, ok, err := r.Events.Event(ctx, t.Symbol, session)
		if err != nil {
			return sum, fmt.Errorf("trigger: read %s %s: %w", t.Symbol, session.Format(time.DateOnly), err)
		}
		if ok && old.SplitPending() {
			r.split(ctx, &sum, all, byResult[t.Symbol])
		}
		if ok && t.Differs(old) {
			sum.Mismatched = append(sum.Mismatched, t.Symbol)
			mismatches.Inc()
			r.Log.Warn("trigger: recorded event differs from a fresh evaluation; the record is kept",
				"symbol", t.Symbol, "session", session.Format(time.DateOnly),
				"recorded_z", old.Z, "fresh_z", t.Z, "recorded_sigma", old.Sigma, "fresh_sigma", t.Sigma,
				"recorded_residual", old.Residual, "fresh_residual", t.Residual)
		}
	}
	r.collectCoverage(ctx, &sum, session)
	r.Log.Info("trigger: run", "session", session.Format(time.DateOnly), "evaluated", sum.Evaluated,
		"skipped", len(sum.Skipped), "trips", len(sum.Trips), "inserted", sum.Inserted, "existing", sum.Existing)
	return sum, nil
}

// split attempts the currency split for a recorded event whose split is
// pending, and writes it once. A final answer (including "no split") is stored;
// a transient failure is logged and counted and leaves the event pending for
// the next run. It never fails the run: the trip is already recorded.
func (r *Runner) split(ctx context.Context, sum *Summary, all map[string]Closes, res Result) {
	if r.Rates == nil {
		return
	}
	date := res.Date.Format(time.DateOnly)
	out, final := r.splitFor(ctx, all, res)
	if !final {
		return
	}
	body, err := json.Marshal(out)
	if err != nil {
		r.Log.Error("trigger: marshal split", "symbol", res.Symbol, "error", err)
		return
	}
	var label *string
	if out.Label != "" {
		label = &out.Label
	}
	if _, err := r.Events.SetSplit(ctx, res.Symbol, res.Date, label, body); err != nil {
		splits.WithLabelValues("error").Inc()
		r.Log.Error("trigger: record split", "symbol", res.Symbol, "session", date, "error", err)
		return
	}
	sum.Splits[res.Symbol] = out
	if out.Status == "none" {
		splits.WithLabelValues("none").Inc()
		r.Log.Info("trigger: no split", "symbol", res.Symbol, "session", date, "reason", out.Reason)
		return
	}
	splits.WithLabelValues(out.Label).Inc()
	r.Log.Info("trigger: split", "symbol", res.Symbol, "session", date, "label", out.Label,
		"currency_z", out.CurrencyPart.Z, "stock_z", out.StockPart.Z)
}

// splitFor computes the split; final is false when the attempt failed in a
// way worth retrying.
func (r *Runner) splitFor(ctx context.Context, all map[string]Closes, res Result) (out SplitResult, final bool) {
	rates, err := r.Rates.Rates(ctx, res.Symbol, res.Date)
	if err != nil {
		if reason, ok := IsNoSplit(err); ok {
			return None(reason), true
		}
		splits.WithLabelValues("error").Inc()
		r.Log.Warn("trigger: split pending, rates unavailable", "symbol", res.Symbol, "session", res.Date.Format(time.DateOnly), "error", err)
		return SplitResult{}, false
	}
	sessions := Sessions(all)
	at, found := slices.BinarySearchFunc(sessions, res.Date, func(a, b time.Time) int { return a.Compare(b) })
	if !found {
		return None(NoSplitNotPublished), true
	}
	var bc Closes
	if res.Benchmark != "" {
		bc = all[res.Benchmark]
	}
	out, err = Split(sessions, at, res.Symbol, res.Benchmark, all[res.Symbol], bc, rates, r.SplitThreshold)
	if err != nil {
		splits.WithLabelValues("error").Inc()
		r.Log.Error("trigger: split", "symbol", res.Symbol, "error", err)
		return SplitResult{}, false
	}
	return out, true
}

// collectCoverage fetches the news and filings for the session's trips that
// have none, and the follow-up (or a retry) for earlier events whose coverage is
// not complete. A failure is logged and left for the next run; the trips are
// already recorded.
func (r *Runner) collectCoverage(ctx context.Context, sum *Summary, session time.Time) {
	if r.Coverage == nil {
		return
	}
	for _, t := range sum.Trips {
		ev := r.currentEvent(ctx, t, sum)
		if ev.Coverage != nil {
			continue
		}
		r.cover(ctx, sum, ev, nil, StageTrip)
	}
	pending, err := r.Events.PendingCoverage(ctx, session)
	if err != nil {
		r.Log.Error("trigger: pending coverage", "error", err)
		return
	}
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	for _, ev := range pending {
		if ev.CoverageFetchedAt != nil && now().Sub(*ev.CoverageFetchedAt) < RetryGap {
			continue
		}
		stage := StageFollowUp
		if ev.Coverage == nil {
			stage = StageTrip // the trip's own fetch never landed
		}
		r.cover(ctx, sum, ev, ev.Coverage, stage)
	}
}

// currentEvent is the recorded event for a trip, or, where nothing is recorded
// (a dry run), the trip itself with this run's split.
func (r *Runner) currentEvent(ctx context.Context, t Trip, sum *Summary) Trip {
	if ev, ok, err := r.Events.Event(ctx, t.Symbol, t.SessionDate); err == nil && ok {
		return ev
	}
	if sp, ok := sum.Splits[t.Symbol]; ok {
		if b, err := json.Marshal(sp); err == nil {
			t.Split = b
			if sp.Label != "" {
				l := sp.Label
				t.SplitLabel = &l
			}
		}
	}
	return t
}

func (r *Runner) cover(ctx context.Context, sum *Summary, ev Trip, prev []byte, stage string) {
	body, err := r.Coverage.Collect(ctx, ev, prev, stage)
	if err != nil {
		if ctx.Err() == nil {
			r.Log.Error("trigger: coverage", "symbol", ev.Symbol, "session", ev.SessionDate.Format(time.DateOnly), "stage", stage, "error", err)
		}
		return
	}
	if err := r.Events.SetCoverage(ctx, ev.Symbol, ev.SessionDate, body); err != nil {
		r.Log.Error("trigger: record coverage", "symbol", ev.Symbol, "error", err)
		return
	}
	sum.Coverages[ev.Symbol+" "+ev.SessionDate.Format(time.DateOnly)] = body
	r.Log.Info("trigger: coverage", "symbol", ev.Symbol, "session", ev.SessionDate.Format(time.DateOnly), "stage", stage)
}

// ReadOnly wraps st so that nothing written through it reaches st: it answers
// "would this insert" from what is recorded. It is how a dry run decides
// without a dry-run branch in Run.
func ReadOnly(st EventStore) EventStore { return readOnly{inner: st} }

type readOnly struct{ inner EventStore }

func (r readOnly) InsertEvent(ctx context.Context, t Trip) (bool, error) {
	_, exists, err := r.inner.Event(ctx, t.Symbol, t.SessionDate)
	return !exists, err
}

// SetSplit reports that it would set the split, and sets nothing.
func (r readOnly) SetSplit(context.Context, string, time.Time, *string, []byte) (bool, error) {
	return true, nil
}

// SetCoverage reports success and stores nothing.
func (r readOnly) SetCoverage(context.Context, string, time.Time, []byte) error { return nil }

func (r readOnly) PendingCoverage(ctx context.Context, before time.Time) ([]Trip, error) {
	return r.inner.PendingCoverage(ctx, before)
}

func (r readOnly) Event(ctx context.Context, symbol string, session time.Time) (Trip, bool, error) {
	return r.inner.Event(ctx, symbol, session)
}
