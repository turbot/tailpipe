package parquet

import (
	"database/sql"
	"fmt"
	"io"
	"os"

	"github.com/turbot/tailpipe/internal/cmdconfig"
	"github.com/turbot/tailpipe/internal/extensions"
)

// duckdb encapsulates the sql.DB connection to DuckDB. this is used to install and
// load the JSON extension.
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

	// Extract the embedded extension
	extractedExtensionName, err := extractAndPrepareExtension()
	if err != nil {
		return nil, fmt.Errorf("failed to extract extension for loading: %w", err)
	}

	// Load the extracted JSON extension from the temporary file
	// check if it is a local build, then just load the json extension, else use the extracted extension
	// for local builds, the json extension should be available in the system
	var loadStmt string
	if cmdconfig.IsLocal() {
		loadStmt = "LOAD 'json';"
	} else {
		loadStmt = fmt.Sprintf("LOAD '%s';", extractedExtensionName)
	}
	if _, err := db.Exec(loadStmt); err != nil {
		return nil, fmt.Errorf("failed to load JSON extension: %w", err)
	}

	w.DB = db
	return w, nil
}

func extractAndPrepareExtension() (string, error) {
	extension, err := extensions.Extract()
	if err != nil {
		return "", fmt.Errorf("failed to extract extension: %w", err)
	}

	// Create a temporary file to store the extracted extension
	tmpFile, err := os.CreateTemp("", "json.duckdb_extension")
	if err != nil {
		return "", fmt.Errorf("could not create temporary file for duckdb extension: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	// Copy the content from the embedded extension file
	_, err = io.Copy(tmpFile, extension)
	if err != nil {
		return "", fmt.Errorf("could not copy embedded extension content to temporary file: %w", err)
	}
	tmpFile.Close() // Ensure the file is closed before loading

	return tmpFile.Name(), nil
}
