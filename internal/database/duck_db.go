package database

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/turbot/pipe-fittings/v2/filepaths"
	"github.com/turbot/tailpipe/internal/constants"
)

type DuckDbOpt func(*DuckDb)

func WithDuckDbExtensions(extensions []string) DuckDbOpt {
	return func(d *DuckDb) {
		d.extensions = extensions
	}
}
func WithDbFile(filename string) DuckDbOpt {
	return func(d *DuckDb) {
		d.dataSourceName = filename
	}
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

	if len(w.extensions) > 0 {
		// install and load the JSON extension
		if err := w.installAndLoadExtensions(); err != nil {
			return nil, fmt.Errorf("failed to install and load extensions: %w", err)
		}
	}

	w.DB = db
	return w, nil
}

// DuckDb encapsulates the sql.DB connection to DuckDB. This is used to install and
// load the required DuckDB extensions.
type DuckDb struct {
	// duckDb connection
	*sql.DB
	extensions     []string
	dataSourceName string
}

func (d *DuckDb) Query(query string, args ...any) (*sql.Rows, error) {
	rows, err := d.DB.Query(query, args...)
	if err != nil {
		// handle invalid parquet error
		return nil, handleDuckDbError(err)
	}
	return rows, err
}

func (d *DuckDb) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	rows, err := d.DB.QueryContext(ctx, query, args...)
	if err != nil {
		// handle invalid parquet error
		return nil, handleDuckDbError(err)
	}
	return rows, err
}

func (d *DuckDb) Exec(query string, args ...any) (sql.Result, error) {
	result, err := d.DB.Exec(query, args...)
	if err != nil {
		// handle invalid parquet error
		return nil, handleDuckDbError(err)
	}
	return result, err
}
func (d *DuckDb) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	result, err := d.DB.ExecContext(ctx, query, args...)
	if err != nil {
		// handle invalid parquet error
		return nil, handleDuckDbError(err)
	}
	return result, err
}

func (d *DuckDb) installAndLoadExtensions() error {
	if d.DB == nil {
		return fmt.Errorf("testDb is nil")
	}
	if len(d.extensions) == 0 {
		return nil
	}

	// set the extension directory
	if _, err := d.DB.Exec(fmt.Sprintf("SET extension_directory = '%s';", filepaths.EnsurePipesDuckDbExtensionsDir())); err != nil {
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
