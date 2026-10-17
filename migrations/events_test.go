package migrations_test

import (
	"strings"
	"testing"

	"github.com/AkashJam/ticker/migrations"
)

// Migration 000007 holds the ledger. These checks pin the decisions that keep
// it a record (phase15.md step 1): the natural key, no retention, no hypertable,
// no Postgres enums, and no stored country.
func TestEventsMigrationShape(t *testing.T) {
	b, err := migrations.FS.ReadFile("000007_events.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(b)
	for _, want := range []string{
		"PRIMARY KEY (symbol, session_date)",
		"CHECK (split_label IN ('currency-led', 'stock-led', 'both'))",
		"caveats           TEXT[]",
		"coverage          JSONB",
		"split             JSONB",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("up migration lacks %q", want)
		}
	}
	for _, banned := range []string{"create_hypertable", "add_retention_policy", "CREATE TYPE", "country"} {
		for _, line := range strings.Split(sql, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "--") {
				continue
			}
			if strings.Contains(strings.ToLower(line), strings.ToLower(banned)) {
				t.Errorf("up migration contains %q: %s", banned, line)
			}
		}
	}

	d, err := migrations.FS.ReadFile("000007_events.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(d), "DROP TABLE events") {
		t.Error("down migration does not drop events")
	}
}

// Migration 000008 adds the label for a trip no part of which passes alone, and
// leaves the other three.
func TestEventsSplitNeitherMigration(t *testing.T) {
	up, err := migrations.FS.ReadFile("000008_events_split_neither.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	down, err := migrations.FS.ReadFile("000008_events_split_neither.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(up), "CHECK (split_label IN ('currency-led', 'stock-led', 'both', 'neither'))") {
		t.Error("up migration does not allow 'neither'")
	}
	if !strings.Contains(string(down), "CHECK (split_label IN ('currency-led', 'stock-led', 'both'))") ||
		strings.Contains(strings.ReplaceAll(string(down), "--", "\n--"), "neither'))") {
		t.Error("down migration must restore the three labels")
	}
	for _, f := range [][]byte{up, down} {
		if !strings.Contains(string(f), "DROP CONSTRAINT events_split_label_check") {
			t.Error("migration does not replace the label constraint")
		}
	}
}
