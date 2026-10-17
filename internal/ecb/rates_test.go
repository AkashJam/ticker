package ecb

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"testing"
	"testing/fstest"
	"time"

	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/trigger"
)

func d(day int) time.Time { return time.Date(2026, 10, day, 0, 0, 0, 0, time.UTC) }

// stateThrough builds a state whose windows run 1..last October with a rate
// that is 1.25 dollars and 1.5 CAD and 20 MXN per euro, and no window for COP.
func stateThrough(last int) *State {
	st := NewState([]string{"USD", "CAD", "MXN", "CLP", "COP"})
	for day := 1; day <= last; day++ {
		st.Append([]Day{{Date: d(day), Rates: map[string]float64{"USD": 1.25, "CAD": 1.5, "MXN": 20}}})
	}
	return st
}

func TestLocalPerUSD(t *testing.T) {
	st := stateThrough(3)
	cad := st.LocalPerUSD("CAD")
	if math.Abs(cad[d(2)]-1.2) > 1e-12 { // 1.5 per euro / 1.25 dollars per euro
		t.Errorf("CAD per USD = %v, want 1.2", cad[d(2)])
	}
	eur := st.LocalPerUSD("EUR")
	if math.Abs(eur[d(2)]-0.8) > 1e-12 { // one dollar buys 1/1.25 euro
		t.Errorf("EUR per USD = %v, want 0.8", eur[d(2)])
	}
	if len(cad) != 3 || len(eur) != 3 {
		t.Errorf("dates: cad %d, eur %d, want 3", len(cad), len(eur))
	}
	if st.LocalPerUSD("COP") != nil || st.LocalPerUSD("CLP") != nil {
		t.Error("a currency with no window must give no rates")
	}
	if NewState([]string{"CAD"}).LocalPerUSD("CAD") != nil {
		t.Error("with no dollar window there is no cross rate")
	}
}

func TestLocalPerUSDCarriesAGapForward(t *testing.T) {
	st := stateThrough(1)
	st.Append([]Day{{Date: d(4), Rates: map[string]float64{"USD": 1.30, "CAD": 1.56, "MXN": 20}}})
	cad := st.LocalPerUSD("CAD")
	if cad[d(2)] != cad[d(1)] || cad[d(3)] != cad[d(1)] {
		t.Errorf("the unpublished days must carry the last rate: %v", cad)
	}
	if math.Abs(cad[d(4)]-1.2) > 1e-12 {
		t.Errorf("rate on the 4th = %v", cad[d(4)])
	}
}

const rateCountries = `{"rows":[
 {"sensor":"RY","currency":"CAD","euroArea":false},
 {"sensor":"AMX","currency":"MXN","euroArea":false},
 {"sensor":"ING","currency":null,"euroArea":true},
 {"sensor":"BCH","currency":"CLP","euroArea":false},
 {"sensor":"CIB","currency":"COP","euroArea":false},
 {"sensor":null,"currency":"USD","euroArea":false}]}`

func rateSource(t *testing.T, st *State, feed Feed) *RateSource {
	t.Helper()
	cache := &memCache{data: map[string]string{}}
	rs := reference.New(cache, fstest.MapFS{"countries.json": {Data: []byte(rateCountries)}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if st != nil {
		b, _ := json.Marshal(st)
		cache.data[reference.CacheKey(StateName)] = string(b)
	}
	return &RateSource{Store: rs, Feed: feed}
}

func reason(t *testing.T, err error) string {
	t.Helper()
	var ns *trigger.NoSplitError
	if !errors.As(err, &ns) {
		t.Fatalf("err = %v, want a final no-split", err)
	}
	return ns.Reason
}

func TestRateSourceFetchesTheSessionsOwnFix(t *testing.T) {
	feed := &fakeFeed{daily: Day{Date: d(5), Rates: map[string]float64{"USD": 1.30, "CAD": 1.56, "MXN": 21}}}
	rs := rateSource(t, stateThrough(4), feed)

	got, err := rs.Rates(context.Background(), "RY", d(5))
	if err != nil {
		t.Fatal(err)
	}
	if got.Source != "daily" || got.Currency != "CAD" || feed.dailyN != 1 || !got.FixDate.Equal(d(5)) {
		t.Errorf("rates = %+v, daily fetches %d", got, feed.dailyN)
	}
	if math.Abs(got.PerUSD[d(5)]-1.2) > 1e-12 || math.Abs(got.PerUSD[d(4)]-1.2) > 1e-12 {
		t.Errorf("per USD = %v", got.PerUSD)
	}

	mxn, err := rs.Rates(context.Background(), "AMX", d(5))
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(mxn.PerUSD[d(5)]-21/1.30) > 1e-9 || math.Abs(mxn.PerUSD[d(4)]-16) > 1e-9 {
		t.Errorf("MXN per USD = %v", mxn.PerUSD)
	}
}

func TestRateSourceUsesTheStateWhenItAlreadyHoldsTheSession(t *testing.T) {
	feed := &fakeFeed{}
	rs := rateSource(t, stateThrough(5), feed)
	got, err := rs.Rates(context.Background(), "RY", d(5))
	if err != nil || got.Source != "state" || feed.dailyN != 0 {
		t.Fatalf("rates = %+v, err %v, daily fetches %d", got, err, feed.dailyN)
	}
}

func TestRateSourceEuroArea(t *testing.T) {
	feed := &fakeFeed{daily: Day{Date: d(5), Rates: map[string]float64{"USD": 1.25, "CAD": 1.5, "MXN": 20}}}
	got, err := rateSource(t, stateThrough(4), feed).Rates(context.Background(), "ING", d(5))
	if err != nil || got.Currency != "EUR" || math.Abs(got.PerUSD[d(5)]-0.8) > 1e-12 {
		t.Fatalf("rates = %+v, err %v", got, err)
	}
}

func TestRateSourceFinalAnswers(t *testing.T) {
	feed := &fakeFeed{daily: Day{Date: d(5), Rates: map[string]float64{"USD": 1.25, "CAD": 1.5, "MXN": 20}}}
	rs := rateSource(t, stateThrough(4), feed)
	ctx := context.Background()

	if _, err := rs.Rates(ctx, "CAT", d(5)); reason(t, err) != trigger.NoSplitNoHomeCurrency {
		t.Error("a global name has no home currency")
	}
	if _, err := rs.Rates(ctx, "CIB", d(5)); reason(t, err) != trigger.NoSplitNoECBCurrency {
		t.Error("COP is not published")
	}
	if _, err := rs.Rates(ctx, "BCH", d(5)); reason(t, err) != trigger.NoSplitMonthlyOnly {
		t.Error("CLP has only a monthly series")
	}
	if feed.dailyN != 0 {
		t.Errorf("a final answer must not cost a fetch (%d)", feed.dailyN)
	}

	// The ECB has not published the session's fix (a TARGET holiday).
	holiday := &fakeFeed{daily: Day{Date: d(2), Rates: map[string]float64{"USD": 1.25, "CAD": 1.5}}}
	if _, err := rateSource(t, stateThrough(2), holiday).Rates(ctx, "RY", d(3)); reason(t, err) != trigger.NoSplitNotPublished {
		t.Error("an unpublished fix is final")
	}
}

func TestRateSourceTransientFailures(t *testing.T) {
	ctx := context.Background()
	down := &fakeFeed{dailyErr: errors.New("ecb down")}
	_, err := rateSource(t, stateThrough(4), down).Rates(ctx, "RY", d(5))
	if err == nil {
		t.Fatal("want an error")
	}
	var ns *trigger.NoSplitError
	if errors.As(err, &ns) {
		t.Error("an unreachable ECB is not a final answer")
	}

	if _, err := rateSource(t, nil, &fakeFeed{}).Rates(ctx, "RY", d(5)); err == nil || errors.As(err, &ns) {
		t.Errorf("with no stored state the error must be transient: %v", err)
	}

	// A daily file newer than the session means the state is behind, not that
	// the fix was never published.
	ahead := &fakeFeed{daily: Day{Date: d(9), Rates: map[string]float64{"USD": 1.25, "CAD": 1.5}}}
	if _, err := rateSource(t, stateThrough(4), ahead).Rates(ctx, "RY", d(5)); err == nil || errors.As(err, &ns) {
		t.Errorf("a state behind the session must be transient: %v", err)
	}
}
