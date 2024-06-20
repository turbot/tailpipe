package parquet

import (
	"database/sql"
	"fmt"
)

type duckDb struct {
	// duckDb connection
	sql.DB
}

// ctor
func newDuckDb() (*duckDb, error) {
	w := &duckDb{}
	// Connect to DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}

	// Load the JSON extension
	if _, err := db.Exec("LOAD 'json';"); err != nil {
		return nil, fmt.Errorf("failed to load JSON extension: %w", err)
	}
	w.DB = *db
	return w, nil
}
