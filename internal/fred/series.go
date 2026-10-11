package fred

import (
	"fmt"
	"time"

	"github.com/AkashJam/ticker/internal/change"
)

// ToSeries turns observations into a stored series that starts in January 2010
// and has no gaps. step is the months between observations: 1 for monthly, 3
// for quarterly. A series that starts later, or skips a period, is an error
// rather than something to interpolate: a window with a missing point is not
// served.
func ToSeries(obs []Obs, step int) (change.Series, error) {
	first := time.Date(change.FirstYear, time.January, 1, 0, 0, 0, 0, time.UTC)
	i := 0
	for i < len(obs) && obs[i].Date.Before(first) {
		i++
	}
	if i == len(obs) || !obs[i].Date.Equal(first) {
		return change.Series{}, fmt.Errorf("fred: series has no observation for January %d", change.FirstYear)
	}
	s := change.Series{Start: first.Format("2006-01"), Step: step}
	prev := first
	for n, o := range obs[i:] {
		if n > 0 && !o.Date.Equal(prev.AddDate(0, step, 0)) {
			return change.Series{}, fmt.Errorf("fred: gap before %s", o.Date.Format(time.DateOnly))
		}
		s.Values = append(s.Values, o.Value)
		prev = o.Date
	}
	return s, nil
}
