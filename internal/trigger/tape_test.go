package trigger

import (
	"testing"
	"time"
)

func TestBuildTapeGivesTheTriggersZAndTheLastSixtyCloses(t *testing.T) {
	const n = Window + 10
	dates, sym := series(n, func(i int) float64 {
		if i == n-1 {
			return 0.05
		}
		return alt(i)
	})
	bench := Closes{}
	for _, d := range dates {
		bench[d] = 100 // flat
	}
	all := map[string]Closes{"CAT": sym, "RY": sym, "EXI": bench, "IXG": bench}
	tape := BuildTape(all, []string{"CAT", "RY"})
	if !tape.AsOf.Equal(dates[n-1]) || len(tape.Sensors) != 2 {
		t.Fatalf("asOf %s sensors %d", tape.AsOf, len(tape.Sensors))
	}
	cat := tape.Sensors[0]
	if len(cat.Closes) != TapeWindow || len(cat.Dates) != TapeWindow || !cat.Dates[TapeWindow-1].Equal(dates[n-1]) {
		t.Errorf("closes %d, dates %d", len(cat.Closes), len(cat.Dates))
	}
	// The z is the trigger's own: Evaluate on the same data.
	want, err := Evaluate(Sessions(all), n-1, "CAT", "EXI", sym, bench)
	if err != nil {
		t.Fatal(err)
	}
	if cat.Z == nil || cat.Sigma == nil || *cat.Z != want.Z || *cat.Sigma != want.Sigma {
		t.Errorf("tape z %v sigma %v, want %v %v", cat.Z, cat.Sigma, want.Z, want.Sigma)
	}
}

func TestBuildTapeSaysWhyThereIsNoZ(t *testing.T) {
	dates, sym := series(20, alt)
	all := map[string]Closes{"RY": sym, "IXG": sym}
	tape := BuildTape(all, []string{"RY"})
	row := tape.Sensors[0]
	if row.Z != nil || row.Reason != "insufficient_history" || len(row.Closes) != 20 {
		t.Errorf("row = %+v", row)
	}
	if !tape.AsOf.Equal(dates[19]) {
		t.Errorf("asOf = %s", tape.AsOf)
	}

	empty := BuildTape(map[string]Closes{}, []string{"RY"})
	if empty.Sensors[0].Reason != "no_session" || !empty.AsOf.Equal(time.Time{}) || empty.Sensors[0].Closes == nil {
		t.Errorf("empty = %+v", empty)
	}
}
