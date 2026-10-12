package ecb

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/AkashJam/ticker/internal/change"
)

// windowDays is how much carried-forward daily history the state keeps: enough
// for Phase 15's sixty-session σ and for lookbacks, with margin.
const windowDays = 400

// DailyWindow is a currency's trailing carried-forward daily rates.
// Rates[i] is the rate on Start + i days.
type DailyWindow struct {
	Start time.Time `json:"start"`
	Rates []float64 `json:"rates"`
}

// Sum accumulates a month's published rates for its average. Only days the ECB
// actually published count, so the averages are the ECB's own definition and
// match the annual averages it publishes; carry-forward fills the daily window
// and nothing else.
type Sum struct {
	Sum  float64 `json:"sum"`
	Days int     `json:"days"`
}

// State is everything the daily job needs to append one more day without
// re-reading the history. It lives in Redis only; the served dataset is what
// is embedded. Monthly sums, keyed "2010-01", give both the monthly averages
// the change measures use and the annual averages step 4 converts spending at.
type State struct {
	Last   time.Time                 `json:"last"` // the last calendar day included
	Wanted []string                  `json:"wanted"`
	Window map[string]*DailyWindow   `json:"window"`
	Sums   map[string]map[string]Sum `json:"sums"`
}

// NewState starts an empty state for the given currencies.
func NewState(wanted []string) *State {
	w := append([]string(nil), wanted...)
	sort.Strings(w)
	return &State{Wanted: w, Window: map[string]*DailyWindow{}, Sums: map[string]map[string]Sum{}}
}

// Append adds published days, oldest first. Every calendar day between two
// publications takes the previous rate, so weekends and holidays have one; a
// currency with no rate yet is skipped until its first publication.
func (s *State) Append(days []Day) {
	for _, d := range days {
		if !s.Last.IsZero() {
			for gap := s.Last.AddDate(0, 0, 1); gap.Before(d.Date); gap = gap.AddDate(0, 0, 1) {
				s.addDay(gap, nil)
			}
		}
		s.addDay(d.Date, d.Rates)
		s.Last = d.Date
	}
}

func (s *State) addDay(date time.Time, rates map[string]float64) {
	ym := date.Format("2006-01")
	for _, c := range s.Wanted {
		w := s.Window[c]
		rate, published := rates[c]
		switch {
		case published:
		case w != nil && len(w.Rates) > 0:
			rate = w.Rates[len(w.Rates)-1] // carry forward
		default:
			continue
		}
		if w == nil {
			w = &DailyWindow{Start: date}
			s.Window[c] = w
		}
		w.Rates = append(w.Rates, rate)
		if len(w.Rates) > windowDays {
			drop := len(w.Rates) - windowDays
			w.Rates = append([]float64(nil), w.Rates[drop:]...)
			w.Start = w.Start.AddDate(0, 0, drop)
		}
		if !published {
			continue // a carried-forward day is not a publication
		}
		if s.Sums[c] == nil {
			s.Sums[c] = map[string]Sum{}
		}
		m := s.Sums[c][ym]
		m.Sum += rate
		m.Days++
		s.Sums[c][ym] = m
	}
}

func round(v float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(v*p) / p
}

// Currency is one currency's entry in the served dataset. Series is local
// units per €1, monthly averages from January 2010 to the last complete month;
// the route inverts it, so a stronger currency is a positive change. Series is
// nil, with a Reason, when the currency cannot be served.
type Currency struct {
	Series        *change.Series     `json:"series"`
	AsOf          string             `json:"asOf"`
	AnnualAverage map[string]float64 `json:"annualAverage"`
	Source        string             `json:"source"`
	Derived       string             `json:"derived,omitempty"` // "via USD" for Chile
	Reason        string             `json:"reason,omitempty"`
}

// Dataset is the served blob, stored as "ecb" and embedded. Field names are
// finalised with the Zod schemas in step 5.
type Dataset struct {
	GeneratedAt string              `json:"generatedAt"`
	Base        string              `json:"base"`
	AsOf        string              `json:"asOf"` // the latest day the ECB has published
	Currencies  map[string]Currency `json:"currencies"`
}

// Source names where the figures come from.
const Source = "ECB euro foreign exchange reference rates"

func monthEnd(ym string) (time.Time, error) {
	t, err := time.Parse("2006-01", ym)
	if err != nil {
		return time.Time{}, err
	}
	return t.AddDate(0, 1, -1), nil
}

// publishedDays is how many days the ECB published in a month: the most any
// wanted currency has. A currency with fewer was missing for part of it.
func (s *State) publishedDays(ym string) int {
	most := 0
	for _, c := range s.Wanted {
		most = max(most, s.Sums[c][ym].Days)
	}
	return most
}

// monthly builds a currency's series of monthly averages, from its first full
// month on or after January 2010 to the last complete month. A month is
// complete once the state includes its last day, and full for a currency once
// it was published on every day the ECB published. The ECB published no shekel
// for 2010, so Israel's series starts in January 2011 and a window that needs
// 2010 is not served for it: a gap is never filled.
func (s *State) monthly(code string) (change.Series, bool) {
	sums := s.Sums[code]
	var out change.Series
	for m := time.Date(change.FirstYear, time.January, 1, 0, 0, 0, 0, time.UTC); ; m = m.AddDate(0, 1, 0) {
		ym := m.Format("2006-01")
		end, _ := monthEnd(ym)
		if end.After(s.Last) {
			break
		}
		sum, ok := sums[ym]
		if len(out.Values) == 0 {
			if !ok || sum.Days == 0 || sum.Days < s.publishedDays(ym) {
				continue // not yet a full month of data
			}
			out = change.Series{Start: ym, Step: 1}
		} else if !ok || sum.Days == 0 {
			return change.Series{}, false // a gap inside a series is never filled
		}
		out.Values = append(out.Values, round(sum.Sum/float64(sum.Days), 6))
	}
	return out, len(out.Values) > 0
}

func (s *State) annualAverages(code string) map[string]float64 {
	type acc struct {
		sum  float64
		days int
	}
	years := map[int]*acc{}
	for ym, m := range s.Sums[code] {
		y, err := strconv.Atoi(ym[:4])
		if err != nil {
			continue
		}
		if years[y] == nil {
			years[y] = &acc{}
		}
		years[y].sum += m.Sum
		years[y].days += m.Days
	}
	out := make(map[string]float64, len(years))
	for y, a := range years {
		out[strconv.Itoa(y)] = round(a.sum/float64(a.days), 6)
	}
	return out
}

// Compute builds the served dataset from the state. Rates are local units per
// €1. Averages are over the days the ECB published; the current year's runs to
// the latest date only.
func (s *State) Compute(generatedAt time.Time) Dataset {
	ds := Dataset{
		GeneratedAt: generatedAt.UTC().Format(time.RFC3339),
		Base:        "EUR",
		AsOf:        s.Last.Format(time.DateOnly),
		Currencies:  make(map[string]Currency, len(s.Wanted)),
	}
	for _, c := range s.Wanted {
		series, ok := s.monthly(c)
		if !ok {
			ds.Currencies[c] = Currency{AsOf: ds.AsOf, AnnualAverage: map[string]float64{}, Source: Source, Reason: "not published by the ECB"}
			continue
		}
		ds.Currencies[c] = Currency{Series: &series, AsOf: series.AsOf(), AnnualAverage: s.annualAverages(c), Source: Source}
	}
	return ds
}

// ChileSource names Chile's derived figure.
const ChileSource = "OECD via FRED CCUSMA02CLM618N × ECB USD per EUR"

// maxPesoLagMonths is how far behind the ECB the OECD pesos series may be
// before Chile falls back to a stated gap (bearing.md #85).
const maxPesoLagMonths = 3

func monthIndex(s change.Series) int {
	y, m := s.Last()
	return y*12 + m - 1
}

// ViaUSD derives pesos per euro month by month: OECD pesos per dollar times the
// ECB's dollars per euro, over the months both have. The result runs to the
// earlier of the two ends. When the OECD series is more than three months
// behind the ECB's, it is not served and the reason is "stale".
func ViaUSD(pesosPerUSD, usdPerEUR change.Series) (change.Series, string) {
	if monthIndex(usdPerEUR)-monthIndex(pesosPerUSD) > maxPesoLagMonths {
		return change.Series{}, "stale"
	}
	out := change.Series{Start: pesosPerUSD.Start, Step: 1}
	y, m := startYM(pesosPerUSD)
	for i, p := range pesosPerUSD.Values {
		idx := y*12 + m - 1 + i
		u, ok := usdPerEUR.At(idx/12, idx%12+1)
		if !ok {
			break
		}
		out.Values = append(out.Values, round(p*u, 4))
	}
	if len(out.Values) == 0 {
		return change.Series{}, "no overlap"
	}
	return out, ""
}

func startYM(s change.Series) (year, month int) {
	var y, m int
	_, _ = fmt.Sscanf(s.Start, "%d-%d", &y, &m)
	return y, m
}

// DeriveChile fills Chile's entry from the pesos series, or marks it stale.
func DeriveChile(ds *Dataset, code string, pesosPerUSD change.Series) error {
	usd, ok := ds.Currencies["USD"]
	if !ok || usd.Series == nil {
		return fmt.Errorf("ecb: no USD series to derive %s from", code)
	}
	series, reason := ViaUSD(pesosPerUSD, *usd.Series)
	if reason != "" {
		ds.Currencies[code] = Currency{AsOf: ds.AsOf, AnnualAverage: map[string]float64{}, Source: ChileSource, Derived: "via USD", Reason: reason}
		return nil
	}
	ds.Currencies[code] = Currency{Series: &series, AsOf: series.AsOf(), AnnualAverage: map[string]float64{}, Source: ChileSource, Derived: "via USD"}
	return nil
}
