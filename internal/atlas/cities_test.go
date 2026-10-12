package atlas

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/AkashJam/ticker/internal/change"
	"github.com/AkashJam/ticker/internal/ecb"
	"github.com/AkashJam/ticker/internal/fred"
	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/worldbank"
)

// ramp is a series of n values rising 100, 101, 102, … from the given start.
func ramp(start string, step, n int) change.Series {
	v := make([]float64, n)
	for i := range v {
		v[i] = 100 + float64(i)
	}
	return change.Series{Start: start, Step: step, Values: v}
}

func level(v float64, unit string) worldbank.Value {
	return worldbank.Value{Value: &v, Unit: unit, Source: "World Bank X", AsOf: "2025"}
}

func wbRow(prices change.Series) worldbank.Row {
	return worldbank.Row{
		Prices:              worldbank.Prices{Source: "World Bank FP.CPI.TOTL, annual average", AsOf: prices.AsOf(), Series: prices},
		EverydaySpendingEur: level(1840, "EUR/person/month"),
		GniPpp:              level(62870, "USD PPP/person"),
		Unemployment:        level(6.4, "%"),
		LifeExpectancy:      level(84, "years"),
	}
}

func country(slug, code string, cur *string, sensor *string) reference.Country {
	city := slug + "-city"
	return reference.Country{Slug: slug, City: &city, Country: slug, Code: code, Currency: cur, EuroArea: cur == nil,
		Sensor: sensor, Atlas: true, Region: "europe", RegionOrder: 2}
}

// fixture: Italy (euro), Japan (yen) and Israel (no shekel before 2011), all
// complete to 2025 except the World Bank CPI, which stops in 2024.
func cityFixture() ([]reference.Country, fred.Dataset, ecb.Dataset, worldbank.Dataset) {
	cs := []reference.Country{
		country("italy", "IT", nil, nil),
		country("japan", "JP", str("JPY"), str("MUFG")),
		country("israel", "IL", str("ILS"), nil),
	}
	fr := fred.Row{
		Shares:  fred.Measure{Source: "OECD via FRED X", AsOf: "2026-08", Series: ramp("2010-01", 1, 12*16+8)},
		Housing: fred.Measure{Source: "BIS via FRED Y", AsOf: "2026-Q1", Series: ramp("2010-01", 3, 4*16+1)},
	}
	f := fred.Dataset{GeneratedAt: "2026-10-11T10:00:00Z", Rows: map[string]fred.Row{"IT": fr, "JP": fr, "IL": fr}}

	// The yen goes from 100 to 50 per euro: it doubles in euro terms.
	jpy := change.Series{Start: "2010-01", Step: 1, Values: make([]float64, 12*16+9)}
	for i := range jpy.Values {
		jpy.Values[i] = 100
	}
	for i := 12 * 5; i < len(jpy.Values); i++ { // from January 2015
		jpy.Values[i] = 50
	}
	ils := ramp("2011-01", 1, 12*15+9)
	e := ecb.Dataset{GeneratedAt: "2026-10-12T06:00:00Z", Currencies: map[string]ecb.Currency{
		"JPY": {Series: &jpy, Source: ecb.Source},
		"ILS": {Series: &ils, Source: ecb.Source},
	}}
	w := worldbank.Dataset{GeneratedAt: "2026-10-12T07:00:00Z", Rows: map[string]worldbank.Row{
		"IT": wbRow(ramp("2010-01", 12, 16)), "JP": wbRow(ramp("2010-01", 12, 16)), "IL": wbRow(ramp("2010-01", 12, 15)),
	}}
	return cs, f, e, w
}

func TestBuildCutsEveryMeasureAtTheCommonEnd(t *testing.T) {
	cs, f, e, w := cityFixture()
	resp, err := Build(cs, f, e, w)
	if err != nil {
		t.Fatal(err)
	}
	if resp.End != "2024" || len(resp.Windows) != 3 || resp.Windows[0] != "2021" || resp.Windows[2] != "2010" || len(resp.Rows) != 3 {
		t.Fatalf("end %s windows %v rows %d", resp.End, resp.Windows, len(resp.Rows))
	}
	if resp.GeneratedAt != "2026-10-12T07:00:00Z" {
		t.Errorf("generatedAt = %s, want the newest dataset's", resp.GeneratedAt)
	}
	it := resp.Rows[0]
	if it.Slug != "italy" || it.City != "italy-city" || it.Region != "europe" || it.HasCityPage {
		t.Errorf("row = %+v", it)
	}
	// Each series' end point is the common year's, not the series' own end.
	if it.Shares.AsOf != "2024-12" || it.Housing.AsOf != "2024-Q4" || it.Prices.AsOf != "2024" {
		t.Errorf("asOf: %s %s %s", it.Shares.AsOf, it.Housing.AsOf, it.Prices.AsOf)
	}
	// Shares: January 2010 = 100, December 2024 = 100 + 179.
	if got := it.Shares.Change["2010"]; got == nil || *got != 179 {
		t.Errorf("shares 2010-24 = %v, want +179 (cut at 2024, not the series' 2026 end)", got)
	}
	if it.Prices.Change["2010"] == nil || *it.Prices.Change["2010"] != 14 { // 100 → 114
		t.Errorf("prices 2010-24 = %v", it.Prices.Change["2010"])
	}
	for name, m := range map[string]WindowChange{"shares": it.Shares, "housing": it.Housing, "prices": it.Prices} {
		if m.Source == "" || m.Reason != "" || m.Derived != "" || len(m.Change) != 3 {
			t.Errorf("%s = %+v", name, m)
		}
	}
	if it.EverydaySpendingEur.Value == nil || *it.EverydaySpendingEur.Value != 1840 || it.EverydaySpendingEur.Unit == "" || it.LifeExpectancy.Source == "" {
		t.Errorf("levels = %+v", it)
	}
}

func TestYearsCompoundToTheWindow(t *testing.T) {
	cs, f, e, w := cityFixture()
	resp, _ := Build(cs, f, e, w)
	shares := resp.Rows[0].Shares
	if len(shares.Years) != 15 { // 2010 to 2024
		t.Fatalf("years = %v", shares.Years)
	}
	compound := 1.0
	for _, v := range shares.Years {
		compound *= 1 + v/100
	}
	if want := 1 + *shares.Change["2010"]/100; math.Abs(compound-want)/want > 0.01 {
		t.Errorf("years compound to %.3f, window is %.3f", compound, want)
	}
	// Prices are annual: each year against the one before, from 2011.
	if p := resp.Rows[0].Prices.Years; len(p) != 14 || p["2010"] != 0 {
		t.Errorf("prices years = %v, want 2011 to 2024", p)
	}
}

func TestEuroAreaCurrencyIsNullWithAReason(t *testing.T) {
	cs, f, e, w := cityFixture()
	resp, _ := Build(cs, f, e, w)
	c := resp.Rows[0].Currency
	if c.Change != nil || c.Years != nil || c.Reason != "euro" || c.Source != ecb.Source || c.AsOf != "2024-12" {
		t.Errorf("euro currency = %+v", c)
	}
	b, _ := json.Marshal(c)
	if !strings.Contains(string(b), `"change":null`) || !strings.Contains(string(b), `"years":null`) {
		t.Errorf("json = %s, want explicit nulls", b)
	}
}

func TestCurrencyIsInvertedSoAStrongerCurrencyIsPositive(t *testing.T) {
	cs, f, e, w := cityFixture()
	resp, _ := Build(cs, f, e, w)
	jp := resp.Rows[1].Currency
	// 100 → 50 yen per euro from 2015: 2010-24 doubles (+100), 2016-24 is flat.
	if jp.Change["2010"] == nil || *jp.Change["2010"] != 100 || *jp.Change["2016"] != 0 {
		t.Errorf("JPY change = %v, want +100 from 2010 and 0 from 2016", jp.Change)
	}
	if jp.Years["2014"] != 100 { // January 2014 (100) to January 2015 (50)
		t.Errorf("JPY years = %v", jp.Years)
	}
}

func TestIsraelsMissingYearIsAStatedGap(t *testing.T) {
	cs, f, e, w := cityFixture()
	resp, _ := Build(cs, f, e, w)
	il := resp.Rows[2].Currency
	if il.Change["2010"] != nil || il.Change["2016"] == nil || il.Change["2021"] == nil || il.Reason != "unpublished" {
		t.Errorf("ILS = %+v, want 2016 and 2021 served and 2010 null with a reason", il)
	}
	if _, ok := il.Years["2010"]; ok || il.Years["2011"] == 0 {
		t.Errorf("ILS years = %v, want 2011 onward and no 2010", il.Years)
	}
	b, _ := json.Marshal(il.Change)
	if !strings.Contains(string(b), `"2010":null`) {
		t.Errorf("change json = %s, want 2010 as an explicit null", b)
	}
}

func TestChileViaTheDollarAndStale(t *testing.T) {
	cs, f, e, w := cityFixture()
	clp := ramp("2010-01", 1, 12*16+8)
	e.Currencies["CLP"] = ecb.Currency{Series: &clp, Source: ecb.ChileSource, Derived: "via USD"}
	cs = append(cs, country("chile", "CL", str("CLP"), str("BCH")))
	f.Rows["CL"], w.Rows["CL"] = f.Rows["IT"], w.Rows["IT"]

	resp, err := Build(cs, f, e, w)
	if err != nil {
		t.Fatal(err)
	}
	cl := resp.Rows[3].Currency
	if cl.Derived != "via USD" || cl.Source != ecb.ChileSource || cl.Change["2010"] == nil || cl.Reason != "" {
		t.Errorf("Chile = %+v", cl)
	}

	e.Currencies["CLP"] = ecb.Currency{Reason: "stale", Derived: "via USD", Source: ecb.ChileSource}
	resp, err = Build(cs, f, e, w)
	if err != nil {
		t.Fatal(err)
	}
	if st := resp.Rows[3].Currency; st.Change != nil || st.Reason != "stale" || st.Derived != "via USD" || st.AsOf != "2024-12" {
		t.Errorf("stale Chile = %+v", st)
	}
	if resp.End != "2024" {
		t.Errorf("end = %s: a stale Chile must not hold the page back", resp.End)
	}
}

func TestMissingLevelsKeepTheirReason(t *testing.T) {
	cs, f, e, w := cityFixture()
	r := w.Rows["IT"]
	r.GniPpp = worldbank.Value{Source: "World Bank NY.GNP.PCAP.PP.CD", AsOf: "2026-10-12", Reason: "not published"}
	w.Rows["IT"] = r
	resp, _ := Build(cs, f, e, w)
	if g := resp.Rows[0].GniPpp; g.Value != nil || g.Reason != "not published" || g.Source == "" || g.AsOf == "" {
		t.Errorf("income = %+v", g)
	}
}

func TestBuildFailsRatherThanGuess(t *testing.T) {
	cs, f, e, w := cityFixture()
	delete(w.Rows, "JP")
	if _, err := Build(cs, f, e, w); err == nil {
		t.Error("a country missing from a dataset was served")
	}
}

func TestEveryMeasureHasASourceAndAnAsOf(t *testing.T) {
	cs, f, e, w := cityFixture()
	resp, _ := Build(cs, f, e, w)
	for _, r := range resp.Rows {
		for name, m := range map[string]WindowChange{"shares": r.Shares, "housing": r.Housing, "prices": r.Prices, "currency": r.Currency} {
			if m.Source == "" || m.AsOf == "" {
				t.Errorf("%s %s: %+v", r.Slug, name, m)
			}
		}
		for name, v := range map[string]Sourced{"spending": r.EverydaySpendingEur, "income": r.GniPpp, "unemployment": r.Unemployment, "life": r.LifeExpectancy} {
			if v.Source == "" || v.AsOf == "" {
				t.Errorf("%s %s: %+v", r.Slug, name, v)
			}
		}
	}
}
