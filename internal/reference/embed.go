package reference

import (
	"embed"
	"io/fs"
)

//go:embed embedded/*.json
var embeddedFS embed.FS

// Embedded returns the committed reference snapshots, one JSON file per
// dataset. It is the floor Store falls back to when Redis cannot answer.
func Embedded() fs.FS {
	sub, err := fs.Sub(embeddedFS, "embedded")
	if err != nil {
		panic(err) // the directory is embedded at compile time
	}
	return sub
}
