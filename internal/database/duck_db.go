package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"

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
	maxMemoryMb    int
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
		baseDir := filepaths.EnsureCollectionTempDir()
		// Create a unique subdirectory with 'duckdb-' prefix
		// it is important to use a unique directory for each DuckDB instance as otherwise temp files from
		// different instances can conflict with each other, causing memory swapping issues
		uniqueTempDir, err := os.MkdirTemp(baseDir, "duckdb-")
		if err != nil {
			return nil, fmt.Errorf("failed to create unique temp directory: %w", err)
		}
		tempDir = uniqueTempDir
	}

	if _, err := db.Exec("set temp_directory = ?;", tempDir); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("failed to set temp_directory: %w", err)
	}
	if w.maxMemoryMb > 0 {
		if _, err := db.Exec("set max_memory = ? || 'MB';", w.maxMemoryMb); err != nil {
			_ = w.Close()
			return nil, fmt.Errorf("failed to set max_memory: %w", err)
		}
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

// GetTempDir returns the temporary directory configured for DuckDB operations
func (d *DuckDb) GetTempDir() string {
	if d.tempDir == "" {
		return filepaths.EnsureCollectionTempDir()
	}
	return d.tempDir
}

func (d *DuckDb) installAndLoadExtensions() error {
	if d.DB == nil {
		return fmt.Errorf("db is nil")
	}
	if len(d.extensions) == 0 {
		return nil
	}

	// set the extension directory
	if _, err := d.DB.Exec("set extension_directory = ?;", pf.EnsurePipesDuckDbExtensionsDir()); err != nil {
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
