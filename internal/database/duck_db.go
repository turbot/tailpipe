package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	pf "github.com/turbot/pipe-fittings/v2/filepaths"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/filepaths"
)

// DuckDb provides a wrapper around the sql.DB connection to DuckDB with enhanced error handling
// for invalid parquet files. It automatically retries operations when encountering invalid parquet
// files, which can occur when files are being written concurrently. The wrapper also handles
// installation and loading of required DuckDB extensions, and manages the connection lifecycle.
type DuckDb struct {
	// duckDb connection
	*sql.DB
	extensions     []string
	dataSourceName string
	tempDir        string
}

func NewDuckDb(opts ...DuckDbOpt) (*DuckDb, error) {
	w := &DuckDb{}
	for _, opt := range opts {
		opt(w)
	}
	// Connect to DuckDB
	db, err := sql.Open("duckdb", w.dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	w.DB = db

	if len(w.extensions) > 0 {
		// install and load the JSON extension
		if err := w.installAndLoadExtensions(); err != nil {
			return nil, fmt.Errorf(": %w", err)
		}
	}

	// Configure DuckDB's temp directory:
	// - If WithTempDir option was provided, use that directory
	// - Otherwise, use the collection temp directory (a subdirectory in the user's home directory
	//   where temporary files for data collection are stored)
	tempDir := w.tempDir
	if tempDir == "" {
		tempDir = filepaths.EnsureCollectionTempDir()
	}
	if _, err := db.Exec(fmt.Sprintf("SET temp_directory = '%s';", tempDir)); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("failed to set temp_directory: %w", err)
	}

	return w, nil
}

func (d *DuckDb) Query(query string, args ...any) (*sql.Rows, error) {
	return executeWithParquetErrorRetry(func() (*sql.Rows, error) {
		return d.DB.Query(query, args...)
	})
}

func (d *DuckDb) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return executeWithParquetErrorRetry(func() (*sql.Rows, error) {
		return d.DB.QueryContext(ctx, query, args...)
	})
}

func (d *DuckDb) QueryRow(query string, args ...any) *sql.Row {
	row, _ := executeWithParquetErrorRetry(func() (*sql.Row, error) {
		return d.DB.QueryRow(query, args...), nil
	})
	return row
}

func (d *DuckDb) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	row, _ := executeWithParquetErrorRetry(func() (*sql.Row, error) {
		return d.DB.QueryRowContext(ctx, query, args...), nil
	})
	return row
}

func (d *DuckDb) Exec(query string, args ...any) (sql.Result, error) {
	return executeWithParquetErrorRetry(func() (sql.Result, error) {
		return d.DB.Exec(query, args...)
	})
}

func (d *DuckDb) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return executeWithParquetErrorRetry(func() (sql.Result, error) {
		return d.DB.ExecContext(ctx, query, args...)
	})
}

func (d *DuckDb) installAndLoadExtensions() error {
	if d.DB == nil {
		return fmt.Errorf("db is nil")
	}
	if len(d.extensions) == 0 {
		return nil
	}

	// set the extension directory
	if _, err := d.DB.Exec(fmt.Sprintf("SET extension_directory = '%s';", pf.EnsurePipesDuckDbExtensionsDir())); err != nil {
		return fmt.Errorf("failed to set extension_directory: %w", err)
	}

	// install and load the extensions
	for _, extension := range constants.DuckDbExtensions {
		if _, err := d.DB.Exec(fmt.Sprintf("INSTALL '%s'; LOAD '%s';", extension, extension)); err != nil {
			return fmt.Errorf("failed to install and load extension %s: %s", extension, err.Error())
		}
	}

	return nil
}

// executeWithParquetErrorRetry executes a function with retry logic for invalid parquet files.
// When it encounters a DuckDB error indicating an invalid Parquet file (either too short or missing magic bytes),
// it will:
// 1. Check if the error is a known invalid Parquet error pattern
// 2. Rename the invalid file by appending '.invalid' to its name
// 3. Convert the error to an invalidParquetError type
// The function will retry up to 1000 times before giving up, collecting any invalid Parquet errors encountered.
// This high retry count is necessary as large operations may encounter many invalid files during concurrent writes.
func executeWithParquetErrorRetry[T any](fn func() (T, error)) (T, error) {
	var result T
	var partitionErr *partitionError
	var err error // Declare err outside the loop
	maxRetries := 1000

	for attempt := 1; attempt <= maxRetries; attempt++ {
		result, err = fn()
		if err == nil {
			return result, nil
		}

		err = handleDuckDbError(err)
		var i invalidParquetError
		if errors.As(err, &i) {
			if partitionErr == nil {
				partitionErr = newPartitionError(i.table, i.partition, i.date)
			} else {
				partitionErr.addError(i.table, i.partition, i.date)
			}
			continue
		}
		// If we get here, it's not a parquet error, so return it
		return result, err
	}
	// If we've exhausted all retries, return the last error we encountered
	return result, err
}
