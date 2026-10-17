package trigger

import (
	"errors"
	"testing"
	"time"
)

func TestBuildBandEdgesFollowTheBenchmarkAndTheTripsSigma(t *testing.T) {
	// Three sessions. The benchmark returns +2% on the last; the sensor closes
	// 100, 101, 110. Sigma 0.01, threshold 2.5: edges are the previous close x
	// (1 + 0.02 +- 0.025).
	dates := []time.Time{start, start.AddDate(0, 0, 1), start.AddDate(0, 0, 2)}
	sym := Closes{dates[0]: 100, dates[1]: 101, dates[2]: 110}
	bench := Closes{dates[0]: 200, dates[1]: 200, dates[2]: 204}
	got, err := BuildBand(dates, 2, sym, bench, 0.01, 2.5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Dates) != 2 || !got.Dates[0].Equal(dates[1]) || !got.Dates[1].Equal(dates[2]) {
		t.Fatalf("dates = %v, want the two sessions with a return, oldest first", got.Dates)
	}
	// Session 1: benchmark flat: 100 x (1 +- 0.025).
	near2(t, "lower[0]", got.Lower[0], 100*0.975, 1e-9)
	near2(t, "upper[0]", got.Upper[0], 100*1.025, 1e-9)
	// Session 2: 101 x (1 + 0.02 -+ 0.025).
	near2(t, "lower[1]", got.Lower[1], 101*(1+0.02-0.025), 1e-9)
	near2(t, "upper[1]", got.Upper[1], 101*(1+0.02+0.025), 1e-9)
	near2(t, "close[1]", got.Close[1], 110, 0)
	if got.Close[1] <= got.Upper[1] {
		t.Error("110 is above 101 x 1.045 = 105.5: the line leaves the band on the tripped session")
	}
}

func TestBuildBandWithNoBenchmarkIsCentredOnThePreviousClose(t *testing.T) {
	dates := []time.Time{start, start.AddDate(0, 0, 1)}
	sym := Closes{dates[0]: 100, dates[1]: 103}
	got, err := BuildBand(dates, 1, sym, nil, 0.02, 2.5)
	if err != nil {
		t.Fatal(err)
	}
	near2(t, "lower", got.Lower[0], 100*0.95, 1e-9)
	near2(t, "upper", got.Upper[0], 100*1.05, 1e-9)
}

func TestBuildBandIsCappedAtTheWindowAndSkipsGaps(t *testing.T) {
	n := BandWindow + 20
	dates := make([]time.Time, n)
	sym := Closes{}
	for i := range n {
		dates[i] = start.AddDate(0, 0, i)
		sym[dates[i]] = 100 + float64(i)
	}
	delete(sym, dates[n-10]) // a missing bar costs two returns, and is not bridged
	got, err := BuildBand(dates, n-1, sym, nil, 0.01, 2.5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Dates) != BandWindow {
		t.Errorf("%d sessions, want %d", len(got.Dates), BandWindow)
	}
	for i := 1; i < len(got.Dates); i++ {
		if !got.Dates[i].After(got.Dates[i-1]) {
			t.Fatal("not oldest first")
		}
		if got.Dates[i].Equal(dates[n-10]) || got.Dates[i].Equal(dates[n-9]) {
			t.Errorf("%s has no return and must not be drawn", got.Dates[i])
		}
	}
}

func TestBuildBandNeedsSomethingToDraw(t *testing.T) {
	dates := []time.Time{start}
	if _, err := BuildBand(dates, 0, Closes{start: 1}, nil, 0.01, 2.5); !errors.Is(err, ErrNoBand) {
		t.Errorf("err = %v", err)
	}
	if _, err := BuildBand(nil, 5, nil, nil, 0.01, 2.5); !errors.Is(err, ErrNoBand) {
		t.Errorf("err = %v", err)
	}
}
