package migrations_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/migrations"
)

// The sweep universe lives twice — a Go list the sweep reads and a migration
// that seeds the symbols table — and nothing else would notice them drifting.
func TestSweepUniverseMatchesMigration(t *testing.T) {
	b, err := migrations.FS.ReadFile("000003_sweep_universe.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(b)

	if len(source.SweepUniverse) != 28 {
		t.Errorf("SweepUniverse has %d symbols, want 28", len(source.SweepUniverse))
	}
	for _, s := range source.SweepUniverse {
		want := fmt.Sprintf("('%s', '%s', '%s',", s.Symbol, strings.ReplaceAll(s.Name, "'", "''"), s.Type)
		if !strings.Contains(sql, want) {
			t.Errorf("migration 000003 has no row starting %s", want)
		}
	}
	if got := strings.Count(sql, ", false)"); got != len(source.SweepUniverse) {
		t.Errorf("migration seeds %d rows with tracked=false, want %d", got, len(source.SweepUniverse))
	}
}
