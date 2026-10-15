package api

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/source"
)

func sensorsRouter(h *Handlers) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/sensors", h.GetSensors)
	return r
}

func TestSensorsAnswersWithRedisDown(t *testing.T) {
	h := &Handlers{Ref: reference.New(nil, reference.Embedded(), nil), Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	var got []reference.Sensor
	if code := get(t, sensorsRouter(h), "/sensors", &got); code != http.StatusOK {
		t.Fatalf("GET /sensors = %d", code)
	}
	if len(got) != 18 {
		t.Fatalf("%d sensors, want 18", len(got))
	}
	for i, s := range got {
		if s.Order != i+1 {
			t.Errorf("position %d holds %s with order %d", i+1, s.Symbol, s.Order)
		}
	}
	// The mock's examples: a country sensor reads Country · company, a global
	// one reads its theme.
	byTicker := map[string]reference.Sensor{}
	for _, s := range got {
		byTicker[s.Symbol] = s
	}
	if m := byTicker["MUFG"]; m.Country == nil || *m.Country != "Japan" || m.Name != "Mitsubishi UFJ" || m.Theme != nil {
		t.Errorf("MUFG = %+v", m)
	}
	if n := byTicker["NVDA"]; n.Country != nil || n.Theme == nil || *n.Theme != "Semiconductors" {
		t.Errorf("NVDA = %+v", n)
	}
}

func TestSensorsWithoutAStoreIs500NotAPanic(t *testing.T) {
	h := &Handlers{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if code := get(t, sensorsRouter(h), "/sensors", nil); code != http.StatusInternalServerError {
		t.Errorf("GET /sensors with no reference store = %d, want 500", code)
	}
}

// The list is only worth having if it matches the rest of the system: the same
// symbols as the sweep, and every country sensor present in the country table.
func TestSensorsMatchTheSweepUniverseAndCountryTable(t *testing.T) {
	ref := reference.New(nil, reference.Embedded(), nil)
	sensors, err := ref.Sensors(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	countries, err := ref.Countries(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	inSweep := map[string]bool{}
	for _, s := range source.SweepUniverse {
		inSweep[s.Symbol] = true
	}
	have := map[string]bool{}
	for _, s := range sensors {
		if strings.HasPrefix(s.Symbol, "SIM:") || !inSweep[s.Symbol] {
			t.Errorf("%s is not a sweep-universe symbol", s.Symbol)
		}
		if have[s.Symbol] {
			t.Errorf("%s listed twice", s.Symbol)
		}
		have[s.Symbol] = true
	}
	for _, c := range countries {
		if c.Sensor != nil && !have[*c.Sensor] {
			t.Errorf("country %s has sensor %s, missing from sensors.json", c.Country, *c.Sensor)
		}
	}
	var globals int
	for _, s := range sensors {
		if s.Group == "global" {
			globals++
		}
	}
	if globals != 5 {
		t.Errorf("%d global sensors, want 5", globals)
	}
}

// "Thin" is the country table's word and the figure behind it is stored with
// the sensor. They have to agree both ways, or a row would say thin with no
// number, or carry a number with no tag (bearing.md #93).
func TestThinListingsCarryTheirFigureAndDate(t *testing.T) {
	ref := reference.New(nil, reference.Embedded(), nil)
	sensors, err := ref.Sensors(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	got := map[string]float64{}
	for _, s := range sensors {
		thin := s.Liquidity != nil && *s.Liquidity == "thin"
		hasFigure := s.LiquidityUsdM != nil
		if thin != hasFigure {
			t.Errorf("%s: thin=%v but figure present=%v", s.Symbol, thin, hasFigure)
		}
		if hasFigure {
			if *s.LiquidityUsdM <= 0 || s.LiquidityAsOf == nil || *s.LiquidityAsOf == "" {
				t.Errorf("%s: figure %v, as-of %v", s.Symbol, *s.LiquidityUsdM, s.LiquidityAsOf)
			}
			got[s.Symbol] = *s.LiquidityUsdM
		} else if s.LiquidityAsOf != nil {
			t.Errorf("%s: a date with no figure", s.Symbol)
		}
	}
	// The 3-month averages measured on 9 October 2026 (phase8.md), as the mock shows them.
	want := map[string]float64{"KB": 25, "BCH": 13, "TKC": 12, "TLK": 11}
	if len(got) != len(want) {
		t.Fatalf("thin sensors with a figure: %v, want %v", got, want)
	}
	for sym, v := range want {
		if got[sym] != v {
			t.Errorf("%s: %v, want %v", sym, got[sym], v)
		}
	}
}

func TestSensorNotesAreOnlyWhereTheTableNeedsThem(t *testing.T) {
	ref := reference.New(nil, reference.Embedded(), nil)
	sensors, err := ref.Sensors(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, s := range sensors {
		if s.Note != nil {
			got[s.Symbol] = *s.Note
		}
	}
	want := map[string]string{"AMX": "telecom", "TKC": "telecom", "TLK": "telecom", "CIB": "was Bancolombia", "BAP": "files in Bermuda"}
	if len(got) != len(want) {
		t.Fatalf("notes %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: %q, want %q", k, got[k], v)
		}
	}
}
