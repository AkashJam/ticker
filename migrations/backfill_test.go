package migrations_test

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/AkashJam/ticker/internal/backfill"
	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/migrations"
)

var backfillRow = regexp.MustCompile(`\('(\d{4}-\d{2}-\d{2}) 00:00:00\+00', '([A-Z]+)', '1d', [\d.]+, [\d.]+, [\d.]+, [\d.]+, NULL\)`)

// Migration 000006 is generated, so what matters is its shape: every sweep
// symbol with enough closes for σ, no volume, no weekend, and nothing from the
// sweep's own era (ON CONFLICT would shield it, but a row there is a mistake).
func TestBackfillMigrationShape(t *testing.T) {
	b, err := migrations.FS.ReadFile("000006_backfill_daily.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(b)
	rows := backfillRow.FindAllStringSubmatch(sql, -1)
	if got := strings.Count(sql, ", NULL)"); got != len(rows) {
		t.Fatalf("%d value rows, %d match the expected shape (volume must be NULL, interval '1d')", got, len(rows))
	}

	perSymbol := map[string]int{}
	for _, r := range rows {
		day, err := time.Parse(time.DateOnly, r[1])
		if err != nil {
			t.Fatal(err)
		}
		if !day.Before(backfill.Cutoff) {
			t.Errorf("%s %s is on or after the sweep's first bar", r[2], r[1])
		}
		if wd := day.Weekday(); wd == time.Saturday || wd == time.Sunday {
			t.Errorf("%s %s is a %s", r[2], r[1], wd)
		}
		perSymbol[r[2]]++
	}
	for _, s := range source.SweepUniverse {
		if n := perSymbol[s.Symbol]; n < backfill.MinCloses {
			t.Errorf("%s has %d closes, want at least %d", s.Symbol, n, backfill.MinCloses)
		}
	}
	if len(perSymbol) != len(source.SweepUniverse) {
		t.Errorf("%d symbols in the migration, want %d", len(perSymbol), len(source.SweepUniverse))
	}
	if !strings.Contains(sql, "ON CONFLICT (symbol, interval, time) DO NOTHING") {
		t.Error("up migration can overwrite the sweep's bars")
	}

	d, err := migrations.FS.ReadFile("000006_backfill_daily.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range source.SweepUniverse {
		if !strings.Contains(string(d), "'"+s.Symbol+"'") {
			t.Errorf("down migration omits %s", s.Symbol)
		}
	}
	if !strings.Contains(string(d), "time < '2026-10-09") {
		t.Error("down migration is not bounded at the sweep's first bar")
	}
}
