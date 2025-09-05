package database

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	pf "github.com/turbot/pipe-fittings/v2/filepaths"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/filepaths"
)

// DuckDb provides a wrapper around the sql.DB connection to DuckDB with enhanced error handling
// for invalid parquet files. It automatically retries operations when encountering invalid parquet
// files, which can occur when files are being written concurrently. The wrapper also handles
// installation and loading of required DuckDB extensions, and manages the connection lifecycle.
type DuckDb struct {
	// duckDb connection
	*sql.DB
	extensions      []string
	dataSourceName  string
	tempDir         string
	maxMemoryMb     int
	ducklakeEnabled bool
}

func NewDuckDb(opts ...DuckDbOpt) (_ *DuckDb, err error) {
	slog.Info("Initializing DuckDB connection")

	w := &DuckDb{}
	for _, opt := range opts {
		opt(w)
	}
	defer func() {
		if err != nil {
			// If an error occurs during initialization, close the DB connection if it was opened
			if w.DB != nil {
				_ = w.DB.Close()
			}
			w.DB = nil // ensure DB is nil to avoid further operations on a closed connection
		}
	}()

	// Connect to DuckDB
	db, err := sql.Open("duckdb", w.dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	w.DB = db

	// for duckdb, limit connections to 1 - DuckDB is designed for single-connection usage
	w.SetMaxOpenConns(1)

	if len(w.extensions) > 0 {
		// install and load the JSON extension
		if err := w.installAndLoadExtensions(); err != nil {
			return nil, fmt.Errorf(": %w", err)
		}
	}
	if w.ducklakeEnabled {
		if err := w.connectDucklake(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to connect to DuckLake: %w", err)
		}

		// Set the default catalog to tailpipe_ducklake to avoid catalog context issues
		if _, err := db.Exec(`use "tailpipe_ducklake"`); err != nil {
			return nil, fmt.Errorf("failed to set default catalog: %w", err)
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
	for _, extension := range pconstants.DuckDbExtensions {
		if _, err := d.DB.Exec(fmt.Sprintf("INSTALL '%s'; LOAD '%s';", extension, extension)); err != nil {
			return fmt.Errorf("failed to install and load extension %s: %s", extension, err.Error())
		}
	}

	return nil
}

// connectDucklake connects the given DuckDB connection to DuckLake
func (d *DuckDb) connectDucklake(ctx context.Context) error {
	// we share the same set of commands for tailpipe connection - get init commands and execute them
	commands := GetDucklakeInitCommands()
	for _, cmd := range commands {
		slog.Info(cmd.Description, "command", cmd.Command)
		_, err := d.ExecContext(ctx, cmd.Command)
		if err != nil {
			return fmt.Errorf("%s failed: %w", cmd.Description, err)
		}
	}

	return nil
}

// GetDucklakeInitCommands returns the set of SQL commands required to initialize and connect to DuckLake.
// this is used both for tailpipe to connect to ducklake and also for tailpipe connect to build the init script
// It returns an ordered slice of SQL commands.
func GetDucklakeInitCommands() []SqlCommand {
	attachQuery := fmt.Sprintf(`attach 'ducklake:sqlite:%s' AS %s (
	data_path '%s/', 
	meta_journal_mode 'WAL', 
	meta_synchronous 'NORMAL')`,
		config.GlobalWorkspaceProfile.GetDucklakeDbPath(),
		pconstants.DuckLakeCatalog,
		config.GlobalWorkspaceProfile.GetDataDir())

	commands := []SqlCommand{
		{Description: "install sqlite extension", Command: "install sqlite"},
		// TODO #DL change to using prod extension when stable
		//  https://github.com/turbot/tailpipe/issues/476
		// _, err = db.Exec("install ducklake;")
		{Description: "install ducklake extension", Command: "force install ducklake from core_nightly"},
		{Description: "attach to ducklake database", Command: attachQuery},
	}
	return commands
}
