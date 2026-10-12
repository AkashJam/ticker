package worldbank

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AkashJam/ticker/internal/change"
	"github.com/AkashJam/ticker/internal/ecb"
	"github.com/AkashJam/ticker/internal/reference"
)

// DatasetName is the reference-store key and embedded file for this fetcher.
const DatasetName = "worldbank"

// Value is one sourced figure. A country that lacks it gets a nil Value and a
// Reason, never a stand-in; AsOf is then the fetch date.
type Value struct {
	Value  *float64 `json:"value"`
	Unit   string   `json:"unit,omitempty"`
	Source string   `json:"source"`
	AsOf   string   `json:"asOf"`
	Reason string   `json:"reason,omitempty"`
}

// Prices is the CPI index as an annual series from 2010. The route turns it
// into change over a window and per year (internal/change); no level is served.
type Prices struct {
	Source string        `json:"source"`
	AsOf   string        `json:"asOf"`
	Series change.Series `json:"series"`
}

// Composition is the economic composition the city pages draw.
type Composition struct {
	Agriculture   Value `json:"agriculture"`
	Industry      Value `json:"industry"`
	Services      Value `json:"services"`
	Manufacturing Value `json:"manufacturing"`
}

// Row is one country. Inflation, the one-year rate, and Composition are
// city-page figures: they are not Atlas measures (bearing.md #84).
type Row struct {
	Prices              Prices      `json:"prices"`
	EverydaySpendingEur Value       `json:"everydaySpendingEur"`
	GniPpp              Value       `json:"gniPpp"`
	Unemployment        Value       `json:"unemployment"`
	LifeExpectancy      Value       `json:"lifeExpectancy"`
	Inflation           Value       `json:"inflation"`
	Composition         Composition `json:"composition"`
}

// Dataset is the blob stored as "worldbank". Field names are finalised with the
// Zod schemas in step 5.
type Dataset struct {
	GeneratedAt string         `json:"generatedAt"`
	Rows        map[string]Row `json:"rows"`
}

// IndicatorSource is the one call the refresher needs, so tests can fake the API.
type IndicatorSource interface {
	Indicator(ctx context.Context, codes []string, id string, query url.Values) ([]Obs, error)
}

// Refresher builds the dataset from the World Bank and the stored ECB rates.
type Refresher struct {
	Source   IndicatorSource
	Store    *reference.Store
	Gap      time.Duration // between calls, to stay polite to a free API
	Attempts int           // per call
	Backoff  time.Duration // before a retry, when the server names no wait
	Now      func() time.Time
	Sleep    func(ctx context.Context, d time.Duration) error
	Log      *slog.Logger
}

func NewRefresher(src IndicatorSource, st *reference.Store, log *slog.Logger) *Refresher {
	return &Refresher{
		Source: src, Store: st,
		Gap: 500 * time.Millisecond, Attempts: 4, Backoff: 3 * time.Second,
		Now: time.Now, Sleep: sleepCtx, Log: log,
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

var (
	mrnev    = url.Values{"mrnev": {"1"}}
	cpiRange = url.Values{"date": {"2010:2026"}}
	popRange = url.Values{"date": {"2015:2026"}}
)

// level is a mrnev=1 indicator copied into a Row as a Value.
type level struct {
	id     string
	unit   string
	places int
	dst    func(*Row) *Value
}

var levels = []level{
	{"NY.GNP.PCAP.PP.CD", "USD PPP/person", 0, func(r *Row) *Value { return &r.GniPpp }},
	{"SL.UEM.TOTL.ZS", "%", 1, func(r *Row) *Value { return &r.Unemployment }},
	{"SP.DYN.LE00.IN", "years", 1, func(r *Row) *Value { return &r.LifeExpectancy }},
	{"FP.CPI.TOTL.ZG", "%", 1, func(r *Row) *Value { return &r.Inflation }},
	{"NV.AGR.TOTL.ZS", "% of GDP", 1, func(r *Row) *Value { return &r.Composition.Agriculture }},
	{"NV.IND.TOTL.ZS", "% of GDP", 1, func(r *Row) *Value { return &r.Composition.Industry }},
	{"NV.SRV.TOTL.ZS", "% of GDP", 1, func(r *Row) *Value { return &r.Composition.Services }},
	{"NV.IND.MANF.ZS", "% of GDP", 1, func(r *Row) *Value { return &r.Composition.Manufacturing }},
}

func round(v float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(v*p) / p
}

type countries struct {
	Rows []struct {
		Code string `json:"code"`
	} `json:"rows"`
}

// Build fetches every indicator and assembles the dataset. It is all or
// nothing: one failed call fails the build and the caller keeps whatever it
// already had. A country that merely lacks a value is a null with a reason.
func (r *Refresher) Build(ctx context.Context) (Dataset, error) {
	var cs countries
	if _, err := r.Store.Get(ctx, "countries", &cs); err != nil {
		return Dataset{}, err
	}
	if len(cs.Rows) == 0 {
		return Dataset{}, errors.New("worldbank: no countries")
	}
	codes := make([]string, len(cs.Rows))
	for i, c := range cs.Rows {
		codes[i] = c.Code
	}

	var rates ecb.Dataset
	if _, err := r.Store.Get(ctx, ecb.DatasetName, &rates); err != nil {
		return Dataset{}, err
	}
	usd := rates.Currencies["USD"].AnnualAverage

	got := map[string][]Obs{}
	calls := []struct {
		id    string
		query url.Values
	}{
		{"FP.CPI.TOTL", cpiRange}, {"NE.CON.PRVT.CD", mrnev}, {"SP.POP.TOTL", popRange},
	}
	for _, l := range levels {
		calls = append(calls, struct {
			id    string
			query url.Values
		}{l.id, mrnev})
	}
	for i, c := range calls {
		if i > 0 {
			if err := r.Sleep(ctx, r.Gap); err != nil {
				return Dataset{}, err
			}
		}
		obs, err := r.fetch(ctx, codes, c.id, c.query)
		if err != nil {
			return Dataset{}, fmt.Errorf("worldbank: %s: %w", c.id, err)
		}
		got[c.id] = obs
	}

	now := r.Now().UTC()
	fetched := now.Format(time.DateOnly)
	ds := Dataset{GeneratedAt: now.Format(time.RFC3339), Rows: make(map[string]Row, len(codes))}
	for _, code := range codes {
		ds.Rows[code] = Row{}
	}

	// Prices: the CPI index as an annual series from 2010.
	byCode := map[string][]Obs{}
	for _, o := range got["FP.CPI.TOTL"] {
		byCode[o.Code] = append(byCode[o.Code], o)
	}
	var failed []string
	for _, code := range codes {
		s, err := annual(byCode[code])
		if err != nil {
			r.Log.Warn("worldbank: prices failed", "code", code, "error", err)
			failed = append(failed, code)
			continue
		}
		row := ds.Rows[code]
		row.Prices = Prices{Source: "World Bank FP.CPI.TOTL, annual average", AsOf: s.AsOf(), Series: s}
		ds.Rows[code] = row
	}
	if len(failed) > 0 {
		return Dataset{}, fmt.Errorf("worldbank: no usable CPI series for %s", strings.Join(failed, ","))
	}

	// Levels: the latest published value for each country.
	for _, l := range levels {
		latest := latestBy(got[l.id])
		for _, code := range codes {
			row := ds.Rows[code]
			v := l.dst(&row)
			*v = Value{Unit: l.unit, Source: "World Bank " + l.id}
			if o, ok := latest[code]; ok {
				val := round(o.Value, l.places)
				v.Value, v.AsOf = &val, strconv.Itoa(o.Year)
			} else {
				v.AsOf, v.Reason = fetched, "not published"
			}
			ds.Rows[code] = row
		}
	}

	// Everyday spending: consumption ÷ population of the same year, at that
	// year's ECB annual average, per month.
	cons := latestBy(got["NE.CON.PRVT.CD"])
	pop := map[string]map[int]float64{}
	for _, o := range got["SP.POP.TOTL"] {
		if pop[o.Code] == nil {
			pop[o.Code] = map[int]float64{}
		}
		pop[o.Code][o.Year] = o.Value
	}
	for _, code := range codes {
		row := ds.Rows[code]
		row.EverydaySpendingEur = spending(cons[code], pop[code], usd, fetched)
		ds.Rows[code] = row
	}
	return ds, nil
}

const spendingSource = "World Bank NE.CON.PRVT.CD ÷ SP.POP.TOTL, ECB annual average"

func spending(c Obs, pop map[int]float64, usd map[string]float64, fetched string) Value {
	v := Value{Unit: "EUR/person/month", Source: spendingSource, AsOf: fetched}
	if c.Year == 0 {
		v.Reason = "consumption not published"
		return v
	}
	p, ok := pop[c.Year]
	if !ok || p <= 0 {
		v.Reason = fmt.Sprintf("no population for %d", c.Year)
		return v
	}
	rate, ok := usd[strconv.Itoa(c.Year)]
	if !ok || rate <= 0 {
		v.Reason = fmt.Sprintf("no ECB rate for %d", c.Year)
		return v
	}
	val := round(c.Value/p/rate/12, 0)
	v.Value, v.AsOf = &val, strconv.Itoa(c.Year)
	return v
}

// latestBy keeps each country's most recent observation.
func latestBy(obs []Obs) map[string]Obs {
	out := map[string]Obs{}
	for _, o := range obs {
		if cur, ok := out[o.Code]; !ok || o.Year > cur.Year {
			out[o.Code] = o
		}
	}
	return out
}

// annual turns one country's CPI observations into an annual series from 2010.
// A series that starts later or skips a year is an error: a window with a
// missing point is not served.
func annual(obs []Obs) (change.Series, error) {
	if len(obs) == 0 {
		return change.Series{}, errors.New("no observations")
	}
	sort.Slice(obs, func(i, j int) bool { return obs[i].Year < obs[j].Year })
	if obs[0].Year != change.FirstYear {
		return change.Series{}, fmt.Errorf("starts in %d, not %d", obs[0].Year, change.FirstYear)
	}
	s := change.Series{Start: fmt.Sprintf("%d-01", change.FirstYear), Step: 12}
	for i, o := range obs {
		if i > 0 && o.Year != obs[i-1].Year+1 {
			return change.Series{}, fmt.Errorf("gap before %d", o.Year)
		}
		s.Values = append(s.Values, o.Value)
	}
	return s, nil
}

// Refresh builds the dataset and, only if that succeeded, stores it.
func (r *Refresher) Refresh(ctx context.Context) error {
	ds, err := r.Build(ctx)
	if err != nil {
		return err
	}
	if err := r.Store.Put(ctx, DatasetName, ds); err != nil {
		return err
	}
	r.Log.Info("worldbank: refreshed", "countries", len(ds.Rows))
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
	at, err := time.Parse(time.RFC3339, ds.GeneratedAt)
	return err != nil || r.Now().Sub(at) > maxAge
}

// fetch retries what can succeed on a retry: rate limits, server errors,
// network errors and the API's intermittent empty body.
func (r *Refresher) fetch(ctx context.Context, codes []string, id string, query url.Values) ([]Obs, error) {
	var lastErr error
	for attempt := 1; attempt <= r.Attempts; attempt++ {
		obs, err := r.Source.Indicator(ctx, codes, id, query)
		if err == nil {
			return obs, nil
		}
		lastErr = err
		if ctx.Err() != nil || attempt == r.Attempts {
			break
		}
		var se *StatusError
		var ae *APIError
		wait := r.Backoff
		switch {
		case errors.As(err, &se):
			if !se.Retryable() {
				return nil, err
			}
			if se.RetryAfter > wait {
				wait = se.RetryAfter
			}
		case errors.As(err, &ae), errors.Is(err, ErrNoData):
			return nil, err // retrying cannot help
		}
		// Anything else, including a timeout or an empty body, is worth another try.
		if err := r.Sleep(ctx, wait); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}
