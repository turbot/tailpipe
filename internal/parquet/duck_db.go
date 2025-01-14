package parquet

import (
	"database/sql"
	"fmt"

	"github.com/turbot/pipe-fittings/filepaths"
	"github.com/turbot/tailpipe/internal/constants"
)

// duckdb encapsulates the sql.DB connection to DuckDB. This is used to install and
// load the required DuckDB extensions.
type duckDb struct {
	// duckDb connection
	*sql.DB
}

// ctor
func newDuckDb() (*duckDb, error) {
	w := &duckDb{}
	// Connect to DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}

	// install and load the JSON extension
	if err := installAndLoadExtensions(db); err != nil {
		return nil, fmt.Errorf("failed to install and load extensions: %w", err)
	}

	w.DB = db
	return w, nil
}

func installAndLoadExtensions(db *sql.DB) error {
	// set the TEMP directory
	if _, err := db.Exec(fmt.Sprintf("PRAGMA temp_directory='%s';", fmt.Sprintf("%s/collection/duckdb.tmp/tmp", filepaths.GetInternalDir()))); err != nil {
		return fmt.Errorf("failed to set temp_directory: %w", err)
	}
	// set the extension directory
	if _, err := db.Exec(fmt.Sprintf("SET extension_directory = '%s';", filepaths.EnsurePipesDuckDbExtensionsDir())); err != nil {
		return fmt.Errorf("failed to set extension_directory: %w", err)
	}

	// install and load the extensions
	for _, extension := range constants.DuckDbExtensions {
		if _, err := db.Exec(fmt.Sprintf("INSTALL '%s'; LOAD '%s';", extension, extension)); err != nil {
			return fmt.Errorf("failed to install and load extension %s: %s", extension, err.Error())
		}
	}

	return nil
}
