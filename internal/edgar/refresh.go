package edgar

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/AkashJam/ticker/internal/reference"
)

// DatasetName is the served and embedded blob.
const DatasetName = "edgar"

// MaxAge is how old the dataset may be before it counts as stale (phase15.md
// step 5, decision 3). A reorganisation that moves a sensor's CIK is found by
// the daily run; a week without a successful run means nobody would find it.
const MaxAge = 7 * 24 * time.Hour

// The kinds of drift the daily check reports.
const (
	DriftTickerMissing  = "ticker_missing"   // the ticker is not in company_tickers.json
	DriftCIKChanged     = "cik_changed"      // company_tickers.json maps it to a different CIK than last time
	DriftTickerNotFiled = "ticker_not_filed" // the filer's record does not list the ticker
	DriftCountry        = "country_mismatch" // the filer's business country is not the sensor's
	DriftCountryUnknown = "country_unknown"  // EDGAR's string is not in the normalisation table
	DriftFetchFailed    = "fetch_failed"     // the filer's record could not be read
)

var (
	driftGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ticker_edgar_drift",
		Help: "Sensors whose EDGAR identity disagrees with the reference table on the last daily check.",
	})
	successGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ticker_edgar_last_success_timestamp_seconds",
		Help: "When the EDGAR identity dataset was last built without error.",
	})
)

// Entry is one sensor's identity at EDGAR.
type Entry struct {
	CIK     string `json:"cik"`     // ten digits, as EDGAR's submissions URLs spell it
	Name    string `json:"name"`    // the filer
	Country string `json:"country"` // EDGAR's business country, as spelled
	ISO     string `json:"iso"`     // that, normalised; empty if the table does not know it
}

// Drift is one finding of the daily check.
type Drift struct {
	Symbol string `json:"symbol"`
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

// Dataset is the served blob: every sensor's CIK, and what the check found.
type Dataset struct {
	Source      string           `json:"source"`
	GeneratedAt string           `json:"generatedAt"`
	AsOf        string           `json:"asOf"`
	Sensors     map[string]Entry `json:"sensors"`
	Drift       []Drift          `json:"drift"`
}

// Fresh reports whether the dataset was built within maxAge of now.
func (d Dataset) Fresh(now time.Time, maxAge time.Duration) bool {
	at, err := time.Parse(time.RFC3339, d.GeneratedAt)
	return err == nil && now.Sub(at) <= maxAge
}

// DriftError is returned by Refresh after storing a dataset whose check found
// something, so the reference check pings failed and the owner looks.
type DriftError struct{ Findings []Drift }

func (e *DriftError) Error() string {
	parts := make([]string, len(e.Findings))
	for i, f := range e.Findings {
		parts[i] = f.Symbol + " " + f.Kind
	}
	return "edgar: drift: " + strings.Join(parts, ", ")
}

// Source is the two EDGAR reads, so tests can fake them.
type Source interface {
	Tickers(ctx context.Context) (map[string]TickerRow, error)
	Submissions(ctx context.Context, cik int) (Submission, error)
}

// Refresher keeps the "edgar" dataset current.
type Refresher struct {
	Source   Source
	Store    *reference.Store
	Attempts int
	Backoff  time.Duration
	Now      func() time.Time
	Sleep    func(ctx context.Context, d time.Duration) error
	Log      *slog.Logger
}

func NewRefresher(src Source, st *reference.Store, log *slog.Logger) *Refresher {
	return &Refresher{Source: src, Store: st, Attempts: 3, Backoff: 3 * time.Second, Now: time.Now, Sleep: sleepCtx, Log: log}
}

// expectation is what a sensor's EDGAR record should say.
type expectation struct {
	symbol string
	iso    string
}

// expectations lists the eighteen sensors with the country their filer should
// be in. A country sensor's is its row's code, unless the row overrides it
// (BAP files in Bermuda and senses Peru), in which case it is the country the
// row says EDGAR gives. The five global names are US domestic filers.
func (r *Refresher) expectations(ctx context.Context) ([]expectation, error) {
	countries, err := r.Store.Countries(ctx)
	if err != nil {
		return nil, err
	}
	var out []expectation
	for _, c := range countries {
		if c.Sensor == nil {
			continue
		}
		iso := c.Code
		if c.EdgarOverride {
			if c.EdgarCountry == nil {
				return nil, fmt.Errorf("edgar: %s overrides its country but names none", *c.Sensor)
			}
			var ok bool
			if iso, ok = Normalise(*c.EdgarCountry); !ok {
				return nil, fmt.Errorf("edgar: %s: override country %q is not in the normalisation table", *c.Sensor, *c.EdgarCountry)
			}
		}
		out = append(out, expectation{*c.Sensor, iso})
	}
	sensors, err := r.Store.Sensors(ctx)
	if err != nil {
		return nil, err
	}
	for _, s := range sensors {
		if s.Group == "global" {
			out = append(out, expectation{s.Symbol, "US"})
		}
	}
	if len(out) == 0 {
		return nil, errors.New("edgar: no sensors")
	}
	return out, nil
}

// Build fetches company_tickers.json once, then each sensor's record, and
// returns the dataset with the drift it found. A sensor that cannot be checked
// keeps its previous entry and is reported; one whose CIK changed is moved to
// the new CIK only if the new filer passes the same checks.
func (r *Refresher) Build(ctx context.Context) (Dataset, error) {
	want, err := r.expectations(ctx)
	if err != nil {
		return Dataset{}, err
	}
	var prev Dataset
	if _, err := r.Store.Get(ctx, DatasetName, &prev); err != nil {
		r.Log.Warn("edgar: no previous dataset", "error", err)
	}

	var tickers map[string]TickerRow
	if err := r.retry(ctx, func() (err error) { tickers, err = r.Source.Tickers(ctx); return }); err != nil {
		return Dataset{}, err
	}

	now := r.Now().UTC()
	ds := Dataset{
		Source:      "SEC EDGAR company_tickers.json and the submissions API",
		GeneratedAt: now.Format(time.RFC3339),
		AsOf:        now.Format(time.DateOnly),
		Sensors:     map[string]Entry{},
		Drift:       []Drift{},
	}
	for _, w := range want {
		old, hadOld := prev.Sensors[w.symbol]
		row, ok := tickers[w.symbol]
		if !ok {
			ds.Drift = append(ds.Drift, Drift{w.symbol, DriftTickerMissing, "not in company_tickers.json"})
			if hadOld {
				ds.Sensors[w.symbol] = old
			}
			continue
		}
		cik := fmt.Sprintf("%010d", row.CIK)
		changed := hadOld && old.CIK != cik

		var sub Submission
		if err := r.retry(ctx, func() (err error) { sub, err = r.Source.Submissions(ctx, row.CIK); return }); err != nil {
			if ctx.Err() != nil {
				return Dataset{}, ctx.Err()
			}
			ds.Drift = append(ds.Drift, Drift{w.symbol, DriftFetchFailed, err.Error()})
			if hadOld {
				ds.Sensors[w.symbol] = old
			}
			continue
		}

		entry := Entry{CIK: cik, Name: sub.Name, Country: sub.BusinessCountry}
		entry.ISO, _ = Normalise(sub.BusinessCountry)
		clean := true
		if !slices.ContainsFunc(sub.Tickers, func(t string) bool { return strings.EqualFold(t, w.symbol) }) {
			ds.Drift = append(ds.Drift, Drift{w.symbol, DriftTickerNotFiled, fmt.Sprintf("CIK %s lists %v", cik, sub.Tickers)})
			clean = false
		}
		switch {
		case entry.ISO == "":
			ds.Drift = append(ds.Drift, Drift{w.symbol, DriftCountryUnknown, fmt.Sprintf("%q is not in the normalisation table", sub.BusinessCountry)})
			clean = false
		case entry.ISO != w.iso:
			ds.Drift = append(ds.Drift, Drift{w.symbol, DriftCountry, fmt.Sprintf("%q is %s, the sensor's is %s", sub.BusinessCountry, entry.ISO, w.iso)})
			clean = false
		}

		switch {
		case !changed:
			ds.Sensors[w.symbol] = entry
		case clean:
			ds.Drift = append(ds.Drift, Drift{w.symbol, DriftCIKChanged, fmt.Sprintf("%s to %s, adopted: the new filer lists the ticker and is in the right country", old.CIK, cik)})
			ds.Sensors[w.symbol] = entry
		default:
			ds.Drift = append(ds.Drift, Drift{w.symbol, DriftCIKChanged, fmt.Sprintf("%s to %s, not adopted: the new filer fails the check; the last good CIK is kept", old.CIK, cik)})
			ds.Sensors[w.symbol] = old
		}
	}
	return ds, nil
}

// Refresh builds the dataset, stores it, and reports drift as an error so the
// reference scheduler's check pings failed. The dataset is stored either way:
// last-good is what a fetcher should read, and the drift list says what to look at.
func (r *Refresher) Refresh(ctx context.Context) error {
	ds, err := r.Build(ctx)
	if err != nil {
		return err
	}
	if err := r.Store.Put(ctx, DatasetName, ds); err != nil {
		return err
	}
	driftGauge.Set(float64(len(ds.Drift)))
	successGauge.Set(float64(r.Now().Unix()))
	r.Log.Info("edgar: refreshed", "sensors", len(ds.Sensors), "drift", len(ds.Drift))
	if len(ds.Drift) > 0 {
		for _, d := range ds.Drift {
			r.Log.Warn("edgar: drift", "symbol", d.Symbol, "kind", d.Kind, "detail", d.Detail)
		}
		return &DriftError{Findings: ds.Drift}
	}
	return nil
}

// Stale reports whether the stored dataset is missing or older than maxAge.
// The embedded fallback counts as missing: it is a floor, not a refresh.
func (r *Refresher) Stale(ctx context.Context, maxAge time.Duration) bool {
	var ds Dataset
	src, err := r.Store.Get(ctx, DatasetName, &ds)
	if err != nil || src != reference.SourceLive {
		return true
	}
	return !ds.Fresh(r.Now(), maxAge)
}

func (r *Refresher) retry(ctx context.Context, f func() error) error {
	var last error
	for attempt := 1; attempt <= r.Attempts; attempt++ {
		if last = f(); last == nil {
			return nil
		}
		if ctx.Err() != nil || attempt == r.Attempts {
			break
		}
		wait := r.Backoff
		var se *StatusError
		if errors.As(last, &se) {
			if !se.Retryable() {
				return last
			}
			wait = max(wait, se.RetryAfter)
		}
		if err := r.Sleep(ctx, wait); err != nil {
			return err
		}
	}
	return last
}
