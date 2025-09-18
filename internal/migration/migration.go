package migration

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/briandowns/spinner"
	perr "github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/error_helpers"
	"github.com/turbot/tailpipe/internal/filepaths"
)

// StatusType represents different types of migration status messages
type StatusType int

const (
	InitialisationFailed StatusType = iota
	MigrationFailed
	CleanupAfterSuccess
	PartialSuccess
	Success
)

// moveDataToMigrating ensures the migration folder exists and handles any existing migrating folder
func moveDataToMigrating(ctx context.Context, dataDefaultDir, migratingDefaultDir string) error {
	// Ensure the 'migrating' folder exists
	migrationDir := config.GlobalWorkspaceProfile.GetMigratingDir()
	if err := os.MkdirAll(migrationDir, 0755); err != nil {
		return fmt.Errorf("failed to create migration directory: %w", err)
	}

	// If the migrating folder exists, it can't have a db as we already checked - delete it
	if _, err := os.Stat(migratingDefaultDir); err == nil {
		// Directory exists, remove it since we already verified it doesn't contain a db
		if err := os.RemoveAll(migratingDefaultDir); err != nil {
			return fmt.Errorf("failed to remove existing migrating directory: %w", err)
		}
	}

	// Now move the data directory to the migrating directory
	if err := os.Rename(dataDefaultDir, migratingDefaultDir); err != nil {
		return fmt.Errorf("failed to move data to migration area: %w", err)
	}

	return nil
}

// MigrateDataToDucklake performs migration of views from tailpipe.db and associated parquet files
// into the new DuckLake metadata catalog
func MigrateDataToDucklake(ctx context.Context) (err error) {
	var statusMsg string
	var partialMigrated bool

	// Determine source and migration directories
	dataDefaultDir := config.GlobalWorkspaceProfile.GetDataDir()
	migratingDefaultDir := config.GlobalWorkspaceProfile.GetMigratingDir()

	// Prompt the user to confirm migration
	shouldContinue, err := promptUserForMigration(ctx, dataDefaultDir)
	if err != nil {
		return fmt.Errorf("failed to get user confirmation: %w", err)
	}
	if !shouldContinue {
		return context.Canceled
	}

	// if we have a status message, show it at the end
	defer func() {
		if statusMsg != "" {
			if err != nil || partialMigrated {
				// if there is an error or a partial migration, show as warning
				error_helpers.ShowWarning(statusMsg)
			} else {
				// show as info if there is no error, or if it is not a partial migration
				error_helpers.ShowInfo(statusMsg)
			}
		}
	}()

	var matchedTableDirs, unmatchedTableDirs []string

	// if the ~/.tailpipe/data directory has a .db file, it means that this is the first time we are migrating
	// if the ~/.tailpipe/migration/migrating directory has a .db file, it means that this is a resume migration
	initialMigration := hasTailpipeDb(dataDefaultDir)
	continueMigration := hasTailpipeDb(migratingDefaultDir)

	// validate: both should not be true - return that last migration left things in a bad state
	if initialMigration && continueMigration {
		return fmt.Errorf("invalid migration state: found tailpipe.db in both data and migrating directories. This should not happen. Please contact Turbot support for assistance")
	}

	// STEP 1: Check if migration is needed
	// We need to migrate if it is the first time we are migrating or if we are resuming a migration
	if !initialMigration && !continueMigration {
		slog.Info("No migration needed - no tailpipe.db found in data or migrating directory")
		return nil
	}

	// Initialize migration status
	status := NewMigrationStatus(0)

	// add any error to status and write to file before returning
	defer func() {
		if err != nil {
			status.AddError(err)
			if perr.IsContextCancelledError(ctx.Err()) {
				// set cancel status and prune the tree
				_ = onCancelled(status)
			} else {
				status.Finish("FAILED")
			}
		}

		// write the status back
		_ = status.WriteStatusToFile()
	}()

	logPath := filepath.Join(config.GlobalWorkspaceProfile.GetMigrationDir(), "migration.log")

	// Spinner for migration progress
	sp := spinner.New(
		spinner.CharSets[14],
		100*time.Millisecond,
		spinner.WithHiddenCursor(true),
		spinner.WithWriter(os.Stdout),
	)

	sp.Suffix = " Migrating data to DuckLake format"
	sp.Start()
	defer sp.Stop()

	// Choose DB path for discovery
	// If this is the first time we are migrating, we need to use .db file from the ~/.tailpipe/data directory
	// If this is a resume migration, we need to use .db file from the ~/.tailpipe/migration/migrating directory
	var discoveryDbPath string
	if initialMigration {
		discoveryDbPath = filepath.Join(dataDefaultDir, "tailpipe.db")
	} else {
		discoveryDbPath = filepath.Join(migratingDefaultDir, "tailpipe.db")
	}

	sp.Suffix = " Migrating data to DuckLake format: discover tables to migrate"
	// STEP 2: Discover legacy tables and their schemas (from chosen DB path)
	// This returns the list of views and a map of view name to its schema
	views, schemas, err := discoverLegacyTablesAndSchemas(ctx, discoveryDbPath)
	if err != nil {
		statusMsg = getStatus(ctx, InitialisationFailed, "")
		return fmt.Errorf("failed to discover legacy tables: %w", err)
	}

	slog.Info("Views: ", "views", views)
	slog.Info("Schemas: ", "schemas", schemas)

	// STEP 3: If this is the first time we are migrating(tables in ~/.tailpipe/data) then move the whole contents of data dir
	// into ~/.tailpipe/migration/migrating respecting the same folder structure.
	// We do this by simply renaming the directory.
	if initialMigration {
		sp.Suffix = " Migrating data to DuckLake format: moving legacy data to migration area"

		if err := moveDataToMigrating(ctx, dataDefaultDir, migratingDefaultDir); err != nil {
			slog.Error("Failed to move data to migrating directory", "error", err)
			statusMsg = getStatus(ctx, InitialisationFailed, logPath)
			return err
		}
	}

	// STEP 4: We have now moved the data into migrating. We have the list of views from the legacy DB.
	// We now need to find the matching table directories in migrating/default by scanning migrating/
	// for tp_table=* directories.
	// The matching table directories are the ones that have a view in the database.
	// The unmatched table directories are the ones that have data(.parquet files) but no view in the database.
	// We will move these to migrated/default.

	// set the base directory to ~.tailpipe/migration/migrating/
	baseDir := migratingDefaultDir
	matchedTableDirs, unmatchedTableDirs, err = findMatchingTableDirs(baseDir, views)
	if err != nil {
		statusMsg = getStatus(ctx, MigrationFailed, logPath)
		return fmt.Errorf("failed to find matching table directories: %w", err)
	}

	if len(unmatchedTableDirs) > 0 {
		sp.Suffix = " Migrating data to DuckLake format: archiving tables without views"
		// move the unmatched table directories to 'unmigrated'
		if err = archiveUnmatchedDirs(ctx, unmatchedTableDirs); err != nil {
			statusMsg = getStatus(ctx, MigrationFailed, logPath)
			return fmt.Errorf("failed to archive unmatched table directories: %w", err)
		}
	}
	// Initialize status with total tables to migrate
	status.Total = len(matchedTableDirs)
	status.update()

	sp.Suffix = " Migrating data to DuckLake format: counting parquet files"
	// Pre-compute total parquet files across matched directories
	totalFiles, err := countParquetFiles(ctx, matchedTableDirs)
	if err == nil {
		status.TotalFiles = totalFiles
		status.updateFiles()
	}

	sp.Suffix = fmt.Sprintf(" Migrating data to DuckLake format (%d/%d, %0.1f%%) | parquet files (%d/%d)", status.Migrated, status.Total, status.ProgressPercent, status.MigratedFiles, status.TotalFiles)

	updateStatus := func(st *MigrationStatus) {
		sp.Suffix = fmt.Sprintf(" Migrating data to DuckLake format (%d/%d, %0.1f%%) | parquet files (%d/%d)", st.Migrated, st.Total, st.ProgressPercent, st.MigratedFiles, st.TotalFiles)
	}

	// STEP 5: Do Migration: Traverse matched table directories, find leaf nodes with parquet files,
	// and perform INSERT within a transaction. On success, move leaf dir to migrated.
	err = doMigration(ctx, matchedTableDirs, schemas, status, updateStatus)
	// If cancellation arrived during migration, prefer the CANCELLED outcome and do not
	// treat it as a failure (which would incorrectly move tailpipe.db to failed)
	if perr.IsContextCancelledError(ctx.Err()) {
		statusMsg = getStatus(ctx, MigrationFailed, logPath)
		return ctx.Err()
	}

	if err != nil {
		statusMsg = getStatus(ctx, MigrationFailed, logPath)
		return fmt.Errorf("migration failed: %w", err)
	}

	// Post-migration outcomes

	if status.Failed > 0 {
		if err := onFailed(status); err != nil {
			statusMsg = getStatus(ctx, MigrationFailed, logPath)
			return fmt.Errorf("failed to cleanup after failed migration: %w", err)
		}
		partialMigrated = true
		statusMsg = getStatus(ctx, PartialSuccess, logPath)
		return err

	}

	// so we are successful - cleanup
	if err := onSuccessful(status); err != nil {
		statusMsg = getStatus(ctx, CleanupAfterSuccess, logPath)
		return fmt.Errorf("failed to cleanup after successful migration: %w", err)
	}

	// all good!
	statusMsg = getStatus(ctx, Success, logPath)

	return err
}

// promptUserForMigration prompts the user to confirm migration and returns true if they want to continue
func promptUserForMigration(ctx context.Context, dataDir string) (bool, error) {
	// Check if context is already cancelled
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	fmt.Printf("We're about to migrate your data to the Ducklake format.\nIf you'd like a backup, your data folder is located at: %s\n\nContinue? [y/N]: ", dataDir)

	// Use goroutine to read input while allowing context cancellation
	type result struct {
		response string
		err      error
	}

	resultChan := make(chan result, 1)
	go func() {
		reader := bufio.NewReader(os.Stdin)
		response, err := reader.ReadString('\n')
		resultChan <- result{response, err}
	}()

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case res := <-resultChan:
		if res.err != nil {
			return false, fmt.Errorf("failed to read user input: %w", res.err)
		}

		response := strings.TrimSpace(strings.ToLower(res.response))
		return response == "y" || response == "yes", nil
	}
}

// getStatus returns the appropriate status message based on error type and context
// It handles cancellation checking internally and returns the appropriate message
func getStatus(ctx context.Context, msgType StatusType, logPath string) string {
	// Handle cancellation first
	if perr.IsContextCancelledError(ctx.Err()) {
		switch msgType {
		case InitialisationFailed:
			return "Migration cancelled. Migration data cleaned up and all original data files remain unchanged. Migration will automatically resume next time you run Tailpipe.\n"
		default:
			return "Migration cancelled. Migration will automatically resume next time you run Tailpipe.\n"
		}
	}

	// Handle non-cancellation cases
	switch msgType {
	case InitialisationFailed:
		return "Migration initialisation failed\nMigration data cleaned up and all original data files remain unchanged. Migration will automatically resume next time you run Tailpipe.\n"
	case MigrationFailed:
		return fmt.Sprintf("Migration failed\nPlease contact tailpipe support. For details, see %s\n", logPath)
	case CleanupAfterSuccess:
		return fmt.Sprintf("Migration succeeded but cleanup failed\nFor details, see %s\n", logPath)
	case PartialSuccess:
		// TODO puskar improve this message
		return fmt.Sprintf("Your data has been migrated to DuckLake with issues.\nFor details, see %s\n", logPath)
	// success
	default:
		return fmt.Sprintf("Your data has been migrated to DuckLake.\nFor details, see %s\n", logPath)
	}
}

// discoverLegacyTablesAndSchemas enumerates legacy DuckDB views and, for each view, its schema.
// It returns the list of view names and a map of view name to its schema (column->type).
// If the legacy database contains no views, both return values are empty.
func discoverLegacyTablesAndSchemas(ctx context.Context, dbPath string) ([]string, map[string]*schema.TableSchema, error) {
	// open a duckdb connection to the legacy legacyDb
	legacyDb, err := database.NewDuckDb(database.WithDbFile(dbPath))
	if err != nil {
		return nil, nil, err
	}
	defer legacyDb.Close()

	views, err := database.GetLegacyTableViews(ctx, legacyDb)
	if err != nil || len(views) == 0 {
		return []string{}, map[string]*schema.TableSchema{}, err
	}
	if perr.IsContextCancelledError(ctx.Err()) {
		return nil, nil, ctx.Err()
	}

	schemas := make(map[string]*schema.TableSchema)
	for _, v := range views {
		if perr.IsContextCancelledError(ctx.Err()) {
			return nil, nil, ctx.Err()
		}
		// get row count for the view (optional future optimization) and schema
		ts, err := database.GetLegacyTableViewSchema(ctx, v, legacyDb)
		if err != nil {
			continue
		}
		schemas[v] = ts
	}
	return views, schemas, nil
}

// migrateTableDirectory recursively traverses a table directory, finds leaf nodes containing
// parquet files, and for each leaf executes a placeholder INSERT within a transaction.
// On success, it moves the leaf directory from migrating to migrated.
func migrateTableDirectory(ctx context.Context, db *database.DuckDb, tableName string, dirPath string, ts *schema.TableSchema, status *MigrationStatus) error {
	// create the table if not exists
	err := database.EnsureDuckLakeTable(ts.Columns, db, tableName)
	if err != nil {
		// fatal – move table dir to failed and return error
		moveTableDirToFailed(ctx, dirPath)
		return err
	}
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		// fatal – move table dir to failed and return error
		moveTableDirToFailed(ctx, dirPath)
		return err
	}

	var parquetFiles []string
	aggErr := NewMigrationError()
	for _, entry := range entries {
		// early exit on cancellation
		if ctx.Err() != nil {
			aggErr.Append(ctx.Err())
			return aggErr
		}

		if entry.IsDir() {
			subDir := filepath.Join(dirPath, entry.Name())
			if err := migrateTableDirectory(ctx, db, tableName, subDir, ts, status); err != nil {
				// just add to error list and continue with other entries
				aggErr.Append(err)
			}
		}

		if strings.HasSuffix(strings.ToLower(entry.Name()), ".parquet") {
			parquetFiles = append(parquetFiles, filepath.Join(dirPath, entry.Name()))
		}
	}

	// If this directory contains parquet files, treat it as a leaf node for migration
	if len(parquetFiles) > 0 {
		err = migrateParquetFiles(ctx, db, tableName, dirPath, ts, status, parquetFiles)
		if err != nil {
			aggErr.Append(err)
			status.AddError(fmt.Errorf("failed migrating parquet files for table '%s' at '%s': %w", tableName, dirPath, err))
		}
	}

	if aggErr.Len() == 0 {
		return nil
	}
	return aggErr
}

func migrateParquetFiles(ctx context.Context, db *database.DuckDb, tableName string, dirPath string, ts *schema.TableSchema, status *MigrationStatus, parquetFiles []string) error {
	filesInLeaf := len(parquetFiles)
	// Placeholder: validate schema (from 'ts') against parquet files if needed
	slog.Info("Found leaf node with parquet files", "table", tableName, "dir", dirPath, "files", filesInLeaf)

	// Begin transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		moveTableDirToFailed(ctx, dirPath)
		status.OnFilesFailed(filesInLeaf)
		return err
	}

	// Build and execute the parquet insert
	if err := insertFromParquetFiles(ctx, tx, tableName, ts.Columns, parquetFiles); err != nil {
		slog.Debug("Rolling back transaction", "table", tableName, "error", err)
		txErr := tx.Rollback()
		if txErr != nil {
			slog.Error("Transaction rollback failed", "table", tableName, "error", txErr)
		}
		moveTableDirToFailed(ctx, dirPath)
		status.OnFilesFailed(filesInLeaf)
		return err
	}
	// Note: cancellation will be handled by outer logic; if needed, you can check and rollback here.

	if err := tx.Commit(); err != nil {
		slog.Error("Error committing transaction", "table", tableName, "error", err)
		moveTableDirToFailed(ctx, dirPath)
		status.OnFilesFailed(filesInLeaf)
		return err
	}

	slog.Info("Successfully committed transaction", "table", tableName, "dir", dirPath, "files", filesInLeaf)

	if err := os.RemoveAll(dirPath); err != nil {
		slog.Error("Failed to remove leaf directory after successful migration", "table", tableName)
		moveTableDirToFailed(ctx, dirPath)
		status.OnFilesFailed(filesInLeaf)
		return fmt.Errorf("failed to remove leaf directory after migration: %w", err)
	}
	_ = os.Remove(dirPath)
	status.OnFilesMigrated(filesInLeaf)
	slog.Info("Migrated leaf node", "table", tableName, "source", dirPath)
	return nil
}

// move any table directories with no corresponding view to ~/.tailpipe/migration/unmigrated/ - we will not migrate them
func archiveUnmatchedDirs(ctx context.Context, unmatchedTableDirs []string) error {
	for _, d := range unmatchedTableDirs {
		// move to ~/.tailpipe/migration/migrated/
		tname := strings.TrimPrefix(filepath.Base(d), "tp_table=")
		slog.Warn("Table %s has data but no view in database; moving without migration", "table", tname, "dir", d)
		migratingRoot := config.GlobalWorkspaceProfile.GetMigratingDir()
		unmigratedRoot := config.GlobalWorkspaceProfile.GetUnmigratedDir()
		// get the relative path from migrating root to d
		rel, err := filepath.Rel(migratingRoot, d)
		if err != nil {
			return err
		}
		// build a dest path by joining unmigrated root with this relative path
		destPath := filepath.Join(unmigratedRoot, rel)
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return err
		}
		// move the entire directory
		if err := utils.MoveDirContents(ctx, d, destPath); err != nil {
			return err
		}
		err = os.Remove(d)
		if err != nil {
			return err
		}
	}
	return nil
}

// doMigration performs the migration of the matched table directories and updates status
func doMigration(ctx context.Context, matchedTableDirs []string, schemas map[string]*schema.TableSchema, status *MigrationStatus, onUpdate func(*MigrationStatus)) error {
	if onUpdate == nil {
		return fmt.Errorf("onUpdate callback is required")
	}
	ducklakeDb, err := database.NewDuckDb(database.WithDuckLake())
	if err != nil {
		return err
	}
	defer ducklakeDb.Close()

	for _, tableDir := range matchedTableDirs {
		tableName := strings.TrimPrefix(filepath.Base(tableDir), "tp_table=")
		if tableName == "" {
			continue
		}
		ts := schemas[tableName]
		if err := migrateTableDirectory(ctx, ducklakeDb, tableName, tableDir, ts, status); err != nil {
			slog.Warn("Migration failed for table; moving to migration/failed", "table", tableName, "error", err)
			status.OnTableFailed(tableName)
		} else {
			status.OnTableMigrated()
		}
		// update our status
		onUpdate(status)
	}
	return nil
}

// moveTableDirToFailed moves a table directory from migrating to failed, preserving relative path.
func moveTableDirToFailed(ctx context.Context, dirPath string) {
	migratingRoot := config.GlobalWorkspaceProfile.GetMigratingDir()
	failedRoot := config.GlobalWorkspaceProfile.GetMigrationFailedDir()
	rel, err := filepath.Rel(migratingRoot, dirPath)
	if err != nil {
		return
	}
	destDir := filepath.Join(failedRoot, rel)
	err = os.MkdirAll(filepath.Dir(destDir), 0755)
	if err != nil {
		slog.Error("moveTableDirToFailed: Failed to create parent for failed dir", "error", err, "dir", destDir)
		return
	}
	err = utils.MoveDirContents(ctx, dirPath, destDir)
	if err != nil {
		slog.Error("moveTableDirToFailed: Failed to move dir to failed", "error", err, "source", dirPath, "destination", destDir)
		return
	}
	err = os.Remove(dirPath)
	if err != nil {
		slog.Error("moveTableDirToFailed: Failed to remove original dir after move", "error", err, "dir", dirPath)
	}
}

// countParquetFiles walks all matched table directories and counts parquet files
func countParquetFiles(ctx context.Context, dirs []string) (int, error) {
	total := 0
	for _, root := range dirs {
		// early exit on cancellation
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && strings.HasSuffix(strings.ToLower(d.Name()), ".parquet") {
				total++
			}
			return nil
		}); err != nil {
			return 0, err
		}
	}
	return total, nil
}

// insertFromParquetFiles builds and executes an INSERT … SELECT read_parquet(...) for a set of parquet files
func insertFromParquetFiles(ctx context.Context, tx *sql.Tx, tableName string, columns []*schema.ColumnSchema, parquetFiles []string) error {
	var colList []string
	for _, c := range columns {
		colList = append(colList, fmt.Sprintf(`"%s"`, c.ColumnName))
	}
	cols := strings.Join(colList, ", ")
	escape := func(p string) string { return strings.ReplaceAll(p, "'", "''") }
	var fileSQL string
	if len(parquetFiles) == 1 {
		fileSQL = fmt.Sprintf("'%s'", escape(parquetFiles[0]))
	} else {
		var quoted []string
		for _, f := range parquetFiles {
			quoted = append(quoted, fmt.Sprintf("'%s'", escape(f)))
		}
		fileSQL = "[" + strings.Join(quoted, ", ") + "]"
	}
	//nolint:gosec // file paths are sanitized
	query := fmt.Sprintf(`
		insert into "%s" (%s)
		select %s from read_parquet(%s)
	`, tableName, cols, cols, fileSQL)
	_, err := tx.ExecContext(ctx, query)
	return err
}

// onSuccessful handles success outcome: cleans migrating db, prunes empty dirs, prints summary
func onSuccessful(status *MigrationStatus) error {
	// Remove any leftover db in migrating
	if err := os.Remove(filepath.Join(config.GlobalWorkspaceProfile.GetMigratingDir(), "tailpipe.db")); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove leftover migrating db: %w", err)
	}

	// Prune empty dirs in migrating
	if err := filepaths.PruneTree(config.GlobalWorkspaceProfile.GetMigratingDir()); err != nil {
		return fmt.Errorf("failed to prune empty directories in migrating: %w", err)
	}
	status.Finish("SUCCESS")
	return nil
}

// onCancelled handles cancellation outcome: keep migrating db, prune empties, print summary
func onCancelled(status *MigrationStatus) error {
	// Do not move db; just prune empties so tree is clean
	_ = filepaths.PruneTree(config.GlobalWorkspaceProfile.GetMigratingDir())
	status.Finish("CANCELLED")
	return nil
}

// onFailed handles failure outcome: move db to failed, prune empties, print summary
func onFailed(status *MigrationStatus) error {
	status.Finish("INCOMPLETE")

	failedDefaultDir := config.GlobalWorkspaceProfile.GetMigrationFailedDir()
	if err := os.MkdirAll(failedDefaultDir, 0755); err != nil {
		return err
	}
	srcDb := filepath.Join(config.GlobalWorkspaceProfile.GetMigratingDir(), "tailpipe.db")
	if _, err := os.Stat(srcDb); err == nil {
		if err := utils.MoveFile(srcDb, filepath.Join(failedDefaultDir, "tailpipe.db")); err != nil {
			return fmt.Errorf("failed to move legacy db to failed: %w", err)
		}
	}
	_ = filepaths.PruneTree(config.GlobalWorkspaceProfile.GetMigratingDir())
	return nil
}
