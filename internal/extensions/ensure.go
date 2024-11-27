package extensions

import (
	"embed"
	"io/fs"

	"github.com/turbot/pipe-fittings/sperr"
)

var (
	//go:embed json.duckdb_extension
	staticFS embed.FS
)

const (
	embeddedExtensionName = "json.duckdb_extension"
)

func Extract() (fs.File, error) {

	extension, err := staticFS.Open(embeddedExtensionName)
	if err != nil {
		return nil, sperr.WrapWithMessage(err, "could not open embedded duckdb extension")
	}
	defer extension.Close()

	return extension, nil
}
