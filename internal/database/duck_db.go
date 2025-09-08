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
	// create a read only connection to ducklake
	duckLakeReadOnly bool

	// a list of view filters - if this is set, we create a set of views in the database, one per table,
	// applying the specified filter
	// NOTE: if view filters are specified, the connection is set to READ ONLY mode (even if read only option is not set)
	viewFilters []string
}

func NewDuckDb(opts ...DuckDbOpt) (_ *DuckDb, err error) {
	slog.Info("Initializing DuckDB connection")

	d := &DuckDb{}
	for _, opt := range opts {
		opt(d)
	}
	defer func() {
		if err != nil {
			// If an error occurs during initialization, close the DB connection if it was opened
			if d.DB != nil {
				_ = d.DB.Close()
			}
			d.DB = nil // ensure DB is nil to avoid further operations on a closed connection
		}
	}()

	// Connect to DuckDB
	db, err := sql.Open("duckdb", d.dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %d", err)
	}
	d.DB = db

	// for duckdb, limit connections to 1 - DuckDB is designed for single-connection usage
	d.SetMaxOpenConns(1)

	// set the extension directory
	if _, err := d.DB.Exec("set extension_directory = ?;", pf.EnsurePipesDuckDbExtensionsDir()); err != nil {
		return nil, fmt.Errorf("failed to set extension_directory: %w", err)
	}

	if len(d.extensions) > 0 {
		// set extension dir and install any specified extensions
		if err := d.installAndLoadExtensions(); err != nil {
			return nil, fmt.Errorf(": %d", err)
		}
	}
	if d.ducklakeEnabled {
		if err := d.connectDucklake(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to connect to DuckLake: %d", err)
		}
	}

	// view filters are used to create a database with a filtered set of data to query,
	// used to support date filtering for the index command
	if len(d.viewFilters) > 0 {
		err = d.createFilteredViews(d.viewFilters)
		if err != nil {
			return nil, fmt.Errorf("failed to create filtered views: %d", err)
		}
	}

	// Configure DuckDB's temp directory
	if err := d.setTempDir(); err != nil {
		return nil, fmt.Errorf("failed to set DuckDB temp directory: %d", err)
	}
	// set the max memory if specified
	if d.maxMemoryMb > 0 {
		if _, err := db.Exec("set max_memory = ? || 'MB';", d.maxMemoryMb); err != nil {
			_ = d.Close()
			return nil, fmt.Errorf("failed to set max_memory: %d", err)
		}
	}

	return d, nil
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
	commands := GetDucklakeInitCommands(d.duckLakeReadOnly)
	// if there are NO view filters, set the default catalog to ducklake
	// if there are view filters, the views will be created in the default memory catalog so do not change the default
	if len(d.viewFilters) == 0 {
		commands = append(commands, SqlCommand{
			Description: "set default catalog to ducklake",
			Command:     fmt.Sprintf("use %s", pconstants.DuckLakeCatalog),
		})
	}

	for _, cmd := range commands {
		slog.Info(cmd.Description, "command", cmd.Command)
		_, err := d.ExecContext(ctx, cmd.Command)
		if err != nil {
			return fmt.Errorf("%s failed: %w", cmd.Description, err)
		}
	}

	return nil
}

func (d *DuckDb) createFilteredViews(filters []string) error {
	// get the sql to create the views based on the filters
	viewSql, err := GetCreateViewsSql(context.Background(), d, d.viewFilters...)
	if err != nil {
		return fmt.Errorf("failed to get create views sql: %w", err)
	}
	// execute the commands to create the views
	slog.Info("Creating views")
	for _, cmd := range viewSql {
		if _, err := d.Exec(cmd.Command); err != nil {
			return fmt.Errorf("failed to create view: %w", err)
		}
	}
	return nil
}

// Configure DuckDB's temp directory
//   - If WithTempDir option was provided, use that directory
//   - Otherwise, use the collection temp directory (a subdirectory in the user's home directory
//     where temporary files for data collection are stored)
func (d *DuckDb) setTempDir() error {
	tempDir := d.tempDir
	if tempDir == "" {
		baseDir := filepaths.EnsureCollectionTempDir()
		// Create a unique subdirectory with 'duckdb-' prefix
		// it is important to use a unique directory for each DuckDB instance as otherwise temp files from
		// different instances can conflict with each other, causing memory swapping issues
		uniqueTempDir, err := os.MkdirTemp(baseDir, "duckdb-")
		if err != nil {
			return fmt.Errorf("failed to create unique temp directory: %d", err)
		}
		tempDir = uniqueTempDir
	}

	if _, err := d.Exec("set temp_directory = ?;", tempDir); err != nil {
		_ = d.Close()
		return fmt.Errorf("failed to set temp_directory: %d", err)
	}
	return nil
}

// GetDucklakeInitCommands returns the set of SQL commands required to initialize and connect to DuckLake.
// this is used both for tailpipe to connect to ducklake and also for tailpipe connect to build the init script
// It returns an ordered slice of SQL commands.
func GetDucklakeInitCommands(readonly bool) []SqlCommand {
	attachOptions := []string{
		fmt.Sprintf("data_path '%s'", config.GlobalWorkspaceProfile.GetDataDir()),
		"meta_journal_mode 'WAL'",
		"meta_synchronous 'NORMAL'",
		// TODO temp disable timeout
		"meta_busy_timeout 0",
	}
	// if readonly mode is requested, add the option
	if readonly {
		attachOptions = append(attachOptions, "READ_ONLY")
	}
	attachQuery := fmt.Sprintf(`attach 'ducklake:sqlite:%s' AS %s (
	%s)`,
		config.GlobalWorkspaceProfile.GetDucklakeDbPath(),
		pconstants.DuckLakeCatalog,
		strings.Join(attachOptions, ",\n\t"))

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
