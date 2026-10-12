package sweep

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/AkashJam/ticker/internal/source"
)

// DryRunStore lets a Runner make every decision a real run would — real
// quotes, real HasCandle reads — while making it impossible to write: its
// UpsertCandle does nothing. Wrapping the store, not branching inside Run,
// keeps the production path free of a dry-run switch.
type DryRunStore struct{ Inner Store }

// ReadOnly wraps st so that nothing written through it reaches st.
func ReadOnly(st Store) Store { return DryRunStore{Inner: st} }

// UpsertCandle discards the bar. A Runner counts it as written, so in a dry
// run Result.Written means "would write".
func (DryRunStore) UpsertCandle(context.Context, source.Candle) error { return nil }

// HasCandle reads through to the real store, so catch-up answers truthfully.
func (d DryRunStore) HasCandle(ctx context.Context, symbol, interval string, at time.Time) (bool, error) {
	return d.Inner.HasCandle(ctx, symbol, interval, at)
}

// Explain says in a sentence what a Run did, or in a dry run what a real one
// would do. The CLI prints it beneath the one-line summary.
func Explain(res Result, dryRun bool) string {
	session := res.Session.Format(time.DateOnly)
	switch res.State {
	case StateHoliday:
		return "No session today: the quote is for " + session + ", so a scheduled run would write nothing and ping success."
	case StateNotFinal:
		return "The quote's session (" + session + ") is still open: nothing would be written yet."
	case StateUpToDate:
		return "The canary already has its bar for " + session + ": catch-up would do nothing."
	case StateWritten:
		if dryRun {
			return fmt.Sprintf("All %d quotes passed validation: a real run would write %d bars for %s.", res.Written, res.Written, session)
		}
		return fmt.Sprintf("Wrote %d bars for %s.", res.Written, session)
	case StateFailed:
		names := strings.Join(res.Failed, ", ")
		if dryRun {
			return fmt.Sprintf("%d symbol(s) did not fetch or validate (%s): a real run would ping /fail.", len(res.Failed), names)
		}
		return fmt.Sprintf("%d symbol(s) not written (%s).", len(res.Failed), names)
	}
	return string(res.State)
}
