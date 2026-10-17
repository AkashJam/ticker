package trigger

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

// The currency split (bearing.md #90; phase15.md step 4). A sensor is priced in
// dollars, so its residual move is the home market's plus the currency's. The
// ECB's rates, as local currency per dollar, give the currency's part; the
// stock part is what is left of the residual, so the two add to exactly what
// tripped. Each is tested against its own sigma over the sixty sessions before
// the tested day, and counts if it passes SplitThreshold on its own.

// The labels a split can carry. They are the events table's allowed values
// (migration 000008).
const (
	LabelCurrencyLed = "currency-led"
	LabelStockLed    = "stock-led"
	LabelBoth        = "both"
	LabelNeither     = "neither"
)

// The reasons a trip has no split. A closed set: the view owns the wording.
const (
	NoSplitNoHomeCurrency = "no_home_currency" // a global name has no home currency
	NoSplitNoECBCurrency  = "no_ecb_currency"  // the ECB publishes no rate for it (COP, PEN)
	NoSplitMonthlyOnly    = "monthly_only"     // only a monthly series exists (CLP)
	NoSplitNotPublished   = "ecb_not_published"
	NoSplitInsufficient   = "insufficient_history"
	NoSplitZeroSigma      = "zero_sigma"
)

// NoSplitError is a final answer: this trip has no split, for the stated
// reason. Any other error from a RateSource is transient and the split stays
// pending, to be retried by the next run.
type NoSplitError struct{ Reason string }

func (e *NoSplitError) Error() string { return "trigger: no currency split: " + e.Reason }

// RateSet is a sensor's home-currency rates for one split.
type RateSet struct {
	Currency string
	// PerUSD is units of the home currency per US dollar, by calendar date at
	// 00:00 UTC. Days the ECB did not publish carry the previous rate forward.
	PerUSD map[time.Time]float64
	// Source is "daily" when the session's rate was fetched from the ECB's
	// daily file, "state" when the stored state already held it.
	Source string
	// FixDate is the date of the ECB fix the session's rate comes from.
	FixDate time.Time
}

// RateSource supplies the rates for a sensor on a session. It returns a
// *NoSplitError for a final "no split" and any other error for a failure worth
// retrying.
type RateSource interface {
	Rates(ctx context.Context, symbol string, session time.Time) (RateSet, error)
}

// Part is one side of the split.
type Part struct {
	Value float64 `json:"value"`
	Sigma float64 `json:"sigma"`
	Z     float64 `json:"z"`
}

// SplitResult is the JSON stored in events.split.
type SplitResult struct {
	Status string `json:"status"` // "ok" or "none"
	Reason string `json:"reason,omitempty"`

	Label    string `json:"label,omitempty"`
	Currency string `json:"currency,omitempty"`
	Source   string `json:"source,omitempty"`
	FixDate  string `json:"fixDate,omitempty"`
	// RatePrevious and RateCurrent are home currency per dollar at the
	// previous session and the tested session, the inputs to the currency part.
	RatePrevious float64 `json:"ratePrevious,omitempty"`
	RateCurrent  float64 `json:"rateCurrent,omitempty"`
	Residual     float64 `json:"residual,omitempty"`
	CurrencyPart *Part   `json:"currencyPart,omitempty"`
	StockPart    *Part   `json:"stockPart,omitempty"`
	Threshold    float64 `json:"threshold,omitempty"`
	Window       int     `json:"window,omitempty"`
	WindowStart  string  `json:"windowStart,omitempty"`
}

// None is a final "no split" record.
func None(reason string) SplitResult { return SplitResult{Status: "none", Reason: reason} }

// LabelFor names which parts passed. A part counts at |z| > threshold.
func LabelFor(zCurrency, zStock, threshold float64) string {
	c, s := math.Abs(zCurrency) > threshold, math.Abs(zStock) > threshold
	switch {
	case c && s:
		return LabelBoth
	case c:
		return LabelCurrencyLed
	case s:
		return LabelStockLed
	}
	return LabelNeither
}

// currencyReturn is the change in dollars per unit of the home currency from
// the previous session to this one, from local-per-dollar rates: a stronger
// currency is positive.
func currencyReturn(prev, cur float64) float64 { return prev/cur - 1 }

// Split divides the residual of the session at sessions[at] into a currency
// part and a stock part and tests each. It uses the sixty most recent
// sessions before the tested one that have a valid residual and both rates;
// a session whose rate is missing is not bridged. benchmark and bench follow
// Evaluate. A *NoSplitError-shaped result is returned as a SplitResult with
// status "none"; err is only for a malformed call.
func Split(sessions []time.Time, at int, symbol, benchmark string, sym, bench Closes, rates RateSet, threshold float64) (SplitResult, error) {
	if at < 1 || at >= len(sessions) {
		return SplitResult{}, fmt.Errorf("trigger: session index %d out of range", at)
	}
	if benchmark != "" && bench == nil {
		return SplitResult{}, fmt.Errorf("trigger: %s has benchmark %s but no closes for it", symbol, benchmark)
	}
	point := func(i int) (res, c float64, ok bool) {
		pt, ok := pointAt(sessions, i, sym, bench)
		if !ok {
			return 0, 0, false
		}
		cur, ok1 := rates.PerUSD[sessions[i]]
		prev, ok0 := rates.PerUSD[sessions[i-1]]
		if !ok1 || !ok0 || cur <= 0 || prev <= 0 {
			return 0, 0, false
		}
		return pt.Residual, currencyReturn(prev, cur), true
	}

	res, c, ok := point(at)
	if !ok {
		return None(NoSplitNotPublished), nil
	}
	var cs, ss []float64
	var windowStart time.Time
	for i := at - 1; i >= 1 && len(cs) < Window; i-- {
		if r, cc, ok := point(i); ok {
			cs = append(cs, cc)
			ss = append(ss, r-cc)
			windowStart = sessions[i]
		}
	}
	if len(cs) < Window {
		return None(NoSplitInsufficient), nil
	}
	sc, ss2 := SampleStdDev(cs), SampleStdDev(ss)
	if sc == 0 || ss2 == 0 {
		return None(NoSplitZeroSigma), nil
	}
	stock := res - c
	zc, zs := c/sc, stock/ss2
	return SplitResult{
		Status:       "ok",
		Label:        LabelFor(zc, zs, threshold),
		Currency:     rates.Currency,
		Source:       rates.Source,
		FixDate:      rates.FixDate.Format(time.DateOnly),
		RatePrevious: rates.PerUSD[sessions[at-1]],
		RateCurrent:  rates.PerUSD[sessions[at]],
		Residual:     res,
		CurrencyPart: &Part{Value: c, Sigma: sc, Z: zc},
		StockPart:    &Part{Value: stock, Sigma: ss2, Z: zs},
		Threshold:    threshold,
		Window:       Window,
		WindowStart:  windowStart.Format(time.DateOnly),
	}, nil
}

// IsNoSplit reports whether err is a final "no split" and returns its reason.
func IsNoSplit(err error) (string, bool) {
	var ns *NoSplitError
	if errors.As(err, &ns) {
		return ns.Reason, true
	}
	return "", false
}
