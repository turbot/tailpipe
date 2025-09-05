package database

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"

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
	w.DB.SetMaxOpenConns(1)

	if len(w.extensions) > 0 {
		// install and load the JSON extension
		if err := w.installAndLoadExtensions(); err != nil {
			return nil, fmt.Errorf(": %w", err)
		}
	}
	if w.ducklakeEnabled {
		dataDir := config.GlobalWorkspaceProfile.GetDataDir()
		// TODO #DL tactical - for now check env for data dir override
		// remove this for prod release https://github.com/turbot/tailpipe/issues/520
		if envDir := os.Getenv("TAILPIPE_DATA_DIR"); envDir != "" {
			dataDir = envDir
		}

		ducklakeDb := config.GlobalWorkspaceProfile.GetDucklakeDbPath()

		if err := connectDucklake(context.Background(), db, ducklakeDb, dataDir); err != nil {
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

// TODO share logic with connect to build sql for this
func connectDucklake(ctx context.Context, db *sql.DB, dbPath, dataPath string) error {

	// 1. Install sqlite extension
	slog.Info("loading sqlite extension")
	_, err := db.ExecContext(ctx, "install sqlite")
	if err != nil {
		return fmt.Errorf("failed to install sqlite extension: %w", err)
	}

	// 2. Install extensions
	// TODO #DL tactical code for S3 - remove before release https://github.com/turbot/tailpipe/issues/520
	if envDir := os.Getenv("TAILPIPE_DATA_DIR"); strings.HasPrefix(envDir, "s3") {
		slog.Info("loading parquet, httpfs, aws extensions for S3")

		// load aws, http and parquet for S3 support
		_, err = db.ExecContext(ctx, "install parquet")
		if err != nil {
			return fmt.Errorf("failed to install parquet extension: %w", err)
		}
		_, err = db.ExecContext(ctx, "install httpfs")
		if err != nil {
			return fmt.Errorf("failed to install httpfs extension: %w", err)
		}
		_, err = db.ExecContext(ctx, "install aws")
		if err != nil {
			return fmt.Errorf("failed to install aws extension: %w", err)
		}
		slog.Info("loading aws credentials")
		// load aws creds
		_, err = db.ExecContext(ctx, "call load_aws_credentials()")
		if err != nil {
			return fmt.Errorf("failed to load aws credentials: %w", err)
		}

	}

	// TODO #DL change to using prod extension when stable
	//  https://github.com/turbot/tailpipe/issues/476
	// _, err = db.Exec("install ducklake;")
	_, err = db.ExecContext(ctx, "force install ducklake from core_nightly")
	if err != nil {
		return fmt.Errorf("failed to install ducklake nightly extension: %w", err)
	}

	// 3. Attach the sqlite database as my_ducklake
	// NOTE: set journal mode to WAL and synchronous to NORMAL for better performance
	slog.Info("attaching sqlite database", "bbPath", dbPath, "dataPath", dataPath)

	// set journal mode to WAL and synchronous to NORMAL for better performance
	var attachQuery = fmt.Sprintf(`attach 'ducklake:sqlite:%s' AS %s (
	data_path '%s/', 
	meta_journal_mode 'WAL', 
	meta_synchronous 'NORMAL')`,
		dbPath,
		pconstants.DuckLakeCatalog,
		dataPath)
	_, err = db.ExecContext(ctx, attachQuery)
	if err != nil {
		return fmt.Errorf("failed to attach sqlite database: %w", err)
	}

	// TODO #DL figure out appropriate row group size https://github.com/turbot/tailpipe/issues/514
	// 4. Set the row group size for parquet files
	// rowGroupQuery := fmt.Sprintf("call ducklake_set_option('%s', 'parquet_row_group_size', 10000);", constants.DuckLakeCatalog)
	// _, err = db.ExecContext(ctx, rowGroupQuery)
	// if err != nil {
	//	return fmt.Errorf("failed to attach sqlite database: %v", err)
	// }

	return nil
}
