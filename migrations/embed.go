// Package migrations embeds the SQL files in this directory so the
// distroless runtime image (no shell, no filesystem migration scripts) can
// still run `ticker migrate` from the same binary that serves traffic —
// portfolio.md §5 lists migrations/ at the repo root, so this embed lives
// alongside the .sql files it wraps rather than duplicating them under
// internal/.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
