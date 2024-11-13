package parquet

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"time"

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
	now := time.Now()
	fmt.Printf(now.String())
	extension, err := extensions.Extract()
	if err != nil {
		return nil, fmt.Errorf("failed to extract extension: %w", err)
	}

	// Create a temporary file to store the extracted extension
	tmpFile, err := os.CreateTemp("", "json.duckdb_extension")
	if err != nil {
		return nil, fmt.Errorf("could not create temporary file for duckdb extension: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	// Copy the content from the embedded extension file
	_, err = io.Copy(tmpFile, extension)
	if err != nil {
		return nil, fmt.Errorf("could not copy embedded extension content to temporary file: %w", err)
	}
	tmpFile.Close() // Ensure the file is closed before loading
	fmt.Println("Time: ", time.Since(now).String())

	// Load the extracted JSON extension from the temporary file
	loadStmt := fmt.Sprintf("LOAD '%s';", tmpFile.Name())
	if _, err := db.Exec(loadStmt); err != nil {
		return nil, fmt.Errorf("failed to load JSON extension: %w", err)
	}

	w.DB = db
	return w, nil
}
