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
