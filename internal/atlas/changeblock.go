package atlas

import (
	"math"

	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/worldbank"
)

// ChangePoint is one year of an indexed line: 100 in the block's base year.
type ChangePoint struct {
	Year  int     `json:"year"`
	Index float64 `json:"index"`
}

// ChangeRises are the whole-period rises in percent, base year to end year.
type ChangeRises struct {
	Survey   float64 `json:"survey"`
	Prices   float64 `json:"prices"`
	Spending float64 `json:"spending"`
}

// ChangeBlock sets a survey's own figure against consumer prices and average
// spending per person over the same years, each indexed to 100 in the base year
// (M22). It carries rates of change only, never a level: the atlas reads rates,
// not levels, across countries. A city with fewer than two editions of its survey
// on one method has none.
type ChangeBlock struct {
	BaseYear       int                     `json:"baseYear"`
	EndYear        int                     `json:"endYear"`
	Survey         []ChangePoint           `json:"survey"`
	Prices         []ChangePoint           `json:"prices"`
	Spending       []ChangePoint           `json:"spending"`
	Rises          ChangeRises             `json:"rises"`
	Note           string                  `json:"note"` // the method, in a sentence
	SurveySource   *reference.SurveySource `json:"surveySource"`
	PricesSource   string                  `json:"pricesSource"`
	SpendingSource string                  `json:"spendingSource"`
}

// buildChange returns nil unless the survey has two or more editions on one method
// and the World Bank prices and spending series cover at least two of the same
// years. The block ends at the last year all three cover; it is never extended by
// carrying a figure forward.
func buildChange(sv reference.Survey, wb worldbank.Row) *ChangeBlock {
	if len(sv.History) < 2 || wb.Spending.Series == nil {
		return nil
	}
	base := sv.History[0].Year
	end := sv.History[len(sv.History)-1].Year
	if y, _ := wb.Prices.Series.Last(); y < end {
		end = y
	}
	if y, _ := wb.Spending.Series.Last(); y < end {
		end = y
	}

	byYear := map[int]float64{}
	for _, e := range sv.History {
		byYear[e.Year] = e.Value
	}
	b := &ChangeBlock{
		BaseYear: base, EndYear: end, Note: sv.HistoryNote, SurveySource: sv.HistorySource,
		PricesSource: wb.Prices.Source, SpendingSource: wb.Spending.Source,
	}
	var s0, p0, c0 float64
	for y := base; y <= end; y++ {
		sval, sok := byYear[y]
		p, pok := wb.Prices.Series.At(y, 1)
		c, cok := wb.Spending.Series.At(y, 1)
		if !sok || !pok || !cok || p <= 0 || c <= 0 {
			return nil // a year missing from any line: not served, never bridged
		}
		if y == base {
			s0, p0, c0 = sval, p, c
		}
		b.Survey = append(b.Survey, ChangePoint{y, index(sval, s0)})
		b.Prices = append(b.Prices, ChangePoint{y, index(p, p0)})
		b.Spending = append(b.Spending, ChangePoint{y, index(c, c0)})
	}
	if len(b.Survey) < 2 {
		return nil
	}
	last := len(b.Survey) - 1
	b.Rises = ChangeRises{
		Survey: round1(b.Survey[last].Index - 100), Prices: round1(b.Prices[last].Index - 100),
		Spending: round1(b.Spending[last].Index - 100),
	}
	return b
}

func index(v, base float64) float64 { return round1(v / base * 100) }

func round1(v float64) float64 { return math.Round(v*10) / 10 }
