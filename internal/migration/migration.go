package migration

import (
	"context"
	"fmt"
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
	"github.com/turbot/tailpipe/internal/filepaths"
)

func MigrateDataToDucklake(ctx context.Context) error {
	// Determine source and migration directories
	dataDefaultDir := config.GlobalWorkspaceProfile.GetDataDir()
	migratingDefaultDir := config.GlobalWorkspaceProfile.GetMigratingDir()
	// failed dir is derived via GetMigrationFailedDir() where needed

	var matchedTableDirs []string
	status := NewMigrationStatus(0)
	cancelledHandled := false
	defer func() {
		if perr.IsContextCancelledError(ctx.Err()) && !cancelledHandled {
			_ = onCancelled(ctx, status)
		}
	}()

	// if the ~/.tailpipe/data directory has a .db file, it means that this is the first time we are migrating
	// if the ~/.tailpipe/migration/migrating directory has a .db file, it means that this is a resume migration
	initialMigration := hasTailpipeDb(dataDefaultDir)
	continueMigration := hasTailpipeDb(migratingDefaultDir)

	// STEP 1: Check if migration is needed
	// We need to migrate if it is the first time we are migrating or if we are resuming a migration
	if !initialMigration && !continueMigration {
		slog.Info("No migration needed - no tailpipe.db found in data or migrating directory")
		return nil
	}
	if perr.IsContextCancelledError(ctx.Err()) {
		return ctx.Err()
	}

	// Choose DB path for discovery
	// If this is the first time we are migrating, we need to use .db file from the ~/.tailpipe/data directory
	// If this is a resume migration, we need to use .db file from the ~/.tailpipe/migration/migrating directory
	var discoveryDbPath string
	if initialMigration {
		discoveryDbPath = filepath.Join(dataDefaultDir, "tailpipe.db")
	} else {
		discoveryDbPath = filepath.Join(migratingDefaultDir, "tailpipe.db")
	}
	if perr.IsContextCancelledError(ctx.Err()) {
		return ctx.Err()
	}

	// STEP 2: Discover legacy tables and their schemas (from chosen DB path)
	// This returns the list of views and a map of view name to its schema
	views, schemas, err := discoverLegacyTablesAndSchemas(ctx, discoveryDbPath)
	if err != nil {
		return fmt.Errorf("failed to discover legacy tables: %w", err)
	}
	if perr.IsContextCancelledError(ctx.Err()) {
		return ctx.Err()
	}
	slog.Info("Views: ", "views", views)
	slog.Info("Schemas: ", "schemas", schemas)

	// STEP 3: If this is the first time we are migrating(tables in ~/.tailpipe/data) then move the whole contents of data dir
	// into ~/.tailpipe/migration/migrating respecting the same folder structure.
	// First-run: copy tailpipe.db into migrated immediately, then move data contents into migrating
	if initialMigration {
		if err := os.MkdirAll(config.GlobalWorkspaceProfile.GetMigratedDir(), 0755); err != nil {
			return err
		}
		if err := utils.CopyFile(filepath.Join(dataDefaultDir, "tailpipe.db"), filepath.Join(config.GlobalWorkspaceProfile.GetMigratedDir(), "tailpipe.db")); err != nil {
			return fmt.Errorf("failed to copy legacy db to migrated: %w", err)
		}
		if err := utils.MoveDirContents(dataDefaultDir, migratingDefaultDir); err != nil {
			return fmt.Errorf("failed to move data dir contents to migrating: %w", err)
		}
	}
	if perr.IsContextCancelledError(ctx.Err()) {
		return ctx.Err()
	}

	// STEP 4: We have now moved the data into migrating. We have the list of views from the legacy DB.
	// We now need to find the matching table directories in migrating/default by scanning migrating/
	// for tp_table=* directories.
	// The matching table directories are the ones that have a view in the database.
	// The unmatched table directories are the ones that have data(.parquet files) but no view in the database.
	// We will move these to migrated/default.

	// set the base directory to ~.tailpipe/migration/migrating/
	baseDir := migratingDefaultDir
	matchedTableDirs, err = getMatchedTableDirs(ctx, baseDir, views)
	if err != nil {
		return err
	}
	if perr.IsContextCancelledError(ctx.Err()) {
		return ctx.Err()
	}

	// Initialize status with total tables to migrate
	status.Total = len(matchedTableDirs)
	status.update()

	// Spinner for migration progress
	sp := spinner.New(
		spinner.CharSets[14],
		100*time.Millisecond,
		spinner.WithHiddenCursor(true),
		spinner.WithWriter(os.Stdout),
	)
	sp.Suffix = fmt.Sprintf(" Migrating tables to DuckLake (%d/%d, %0.1f%%)", status.Migrated, status.Total, status.ProgressPercent)
	sp.Start()

	updateStatus := func(st MigrationStatus) {
		sp.Suffix = fmt.Sprintf(" Migrating tables to DuckLake (%d/%d, %0.1f%%)", st.Migrated, st.Total, st.ProgressPercent)
	}

	// STEP 5: Do Migration: Traverse matched table directories, find leaf nodes with parquet files,
	// and perform INSERT within a transaction. On success, move leaf dir to migrated.
	if err := doMigration(ctx, matchedTableDirs, schemas, status, updateStatus); err != nil {
		sp.Stop()
		if perr.IsContextCancelledError(err) || perr.IsContextCancelledError(ctx.Err()) {
			_ = onCancelled(ctx, status)
			cancelledHandled = true
			return err
		}
		// Unexpected error — treat as failure outcome
		_ = onFailed(ctx, status)
		return err
	}

	sp.Stop()

	// Post-migration outcomes
	if status.Failed > 0 {
		if err := onFailed(ctx, status); err != nil {
			return err
		}
	} else {
		if err := onSuccessful(ctx, status); err != nil {
			return err
		}
	}

	return nil
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
func migrateTableDirectory(ctx context.Context, db *database.DuckDb, tableName string, dirPath string, ts *schema.TableSchema) error {
	// create the table if not exists
	err := database.EnsureDuckLakeTable(ts.Columns, db, tableName)
	if err != nil {
		// fatal – move table dir to failed and return error
		moveTableDirToFailed(dirPath)
		return err
	}
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		moveTableDirToFailed(dirPath)
		return err
	}

	var parquetFiles []string
	var firstErr error
	for _, entry := range entries {
		if perr.IsContextCancelledError(ctx.Err()) {
			return ctx.Err()
		}
		if entry.IsDir() {
			subDir := filepath.Join(dirPath, entry.Name())
			if err := migrateTableDirectory(ctx, db, tableName, subDir, ts); err != nil {
				// record the first error but continue to process siblings
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			continue
		}
		if strings.HasSuffix(strings.ToLower(entry.Name()), ".parquet") {
			parquetFiles = append(parquetFiles, filepath.Join(dirPath, entry.Name()))
		}
	}

	// If this directory contains parquet files, treat it as a leaf node for migration
	if len(parquetFiles) > 0 {
		// Placeholder: validate schema (from 'ts') against parquet files if needed
		slog.Info("Found leaf node with parquet files", "table", tableName, "dir", dirPath, "files", len(parquetFiles))

		// Begin transaction
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			moveTableDirToFailed(dirPath)
			return err
		}

		// Build explicit column list and insert from read_parquet
		var colList []string
		for _, c := range ts.Columns {
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
		insertQuery := fmt.Sprintf(`
			insert into "%s" (%s)
			select %s from read_parquet(%s)
		`, tableName, cols, cols, fileSQL)
		_, err = tx.ExecContext(ctx, insertQuery)

		if err != nil {
			slog.Debug("Rollbacking transaction", "table", tableName, "error", err)
			_ = tx.Rollback()
			moveTableDirToFailed(dirPath)
			return err
		}
		// Note: cancellation will be handled by outer logic; if needed, you can check and rollback here.

		if err := tx.Commit(); err != nil {
			slog.Error("Committing transaction", "table", tableName, "error", err)
			moveTableDirToFailed(dirPath)
			return err
		}

		slog.Info(">> successfully committed", "table", tableName, "dir", dirPath, "files", len(parquetFiles))

		// On success, move the entire leaf directory from migrating to migrated
		migratingRoot := config.GlobalWorkspaceProfile.GetMigratingDir()
		migratedRoot := config.GlobalWorkspaceProfile.GetMigratedDir()
		rel, err := filepath.Rel(migratingRoot, dirPath)
		if err != nil {
			moveTableDirToFailed(dirPath)
			return err
		}
		destDir := filepath.Join(migratedRoot, rel)
		if err := os.MkdirAll(filepath.Dir(destDir), 0755); err != nil {
			moveTableDirToFailed(dirPath)
			return err
		}
		if err := utils.MoveDirContents(dirPath, destDir); err != nil {
			moveTableDirToFailed(dirPath)
			return err
		}
		_ = os.Remove(dirPath)
		slog.Info("Migrated leaf node", "table", tableName, "source", dirPath, "destination", destDir)
		// fmt.Printf("migrated %s, sleeping for 2 seconds\n", dirPath)
		// time.Sleep(2 * time.Second)
	}

	return firstErr
}

// getMatchedTableDirs returns the list of matched table directories
// any unmatched(no views in db) table directories are moved to ~/.tailpipe/migration/migrated/
func getMatchedTableDirs(ctx context.Context, baseDir string, views []string) ([]string, error) {
	matchedTableDirs, unmatchedTableDirs, err := findMatchingTableDirs(baseDir, views)
	if err != nil {
		return nil, err
	}
	slog.Info("Matched tp_table directories", "dirs", matchedTableDirs)
	for _, d := range unmatchedTableDirs {
		if perr.IsContextCancelledError(ctx.Err()) {
			return matchedTableDirs, ctx.Err()
		}
		// move to ~/.tailpipe/migration/migrated/
		tname := strings.TrimPrefix(filepath.Base(d), "tp_table=")
		slog.Warn("Table %s has data but no view in database; moving without migration", "table", tname, "dir", d)
		migratingRoot := config.GlobalWorkspaceProfile.GetMigratingDir()
		migratedRoot := config.GlobalWorkspaceProfile.GetMigratedDir()
		rel, err := filepath.Rel(migratingRoot, d)
		if err != nil {
			return matchedTableDirs, err
		}
		destPath := filepath.Join(migratedRoot, rel)
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return matchedTableDirs, err
		}
		if err := utils.MoveDirContents(d, destPath); err != nil {
			return matchedTableDirs, err
		}
		_ = os.Remove(d)
	}
	return matchedTableDirs, nil
}

// doMigration performs the migration of the matched table directories and updates status
func doMigration(ctx context.Context, matchedTableDirs []string, schemas map[string]*schema.TableSchema, status *MigrationStatus, onUpdate func(MigrationStatus)) error {
	ducklakeDb, err := database.NewDuckDb(database.WithDuckLake())
	if err != nil {
		return err
	}
	defer ducklakeDb.Close()

	for _, tableDir := range matchedTableDirs {
		if perr.IsContextCancelledError(ctx.Err()) {
			return ctx.Err()
		}
		tableName := strings.TrimPrefix(filepath.Base(tableDir), "tp_table=")
		if tableName == "" {
			continue
		}
		ts := schemas[tableName]
		if err := migrateTableDirectory(ctx, ducklakeDb, tableName, tableDir, ts); err != nil {
			slog.Warn("Migration failed for table; moving to migration/failed", "table", tableName, "error", err)
			status.OnTableFailed(tableName)
			if onUpdate != nil {
				onUpdate(*status)
			}
			continue
		}
		status.OnTableMigrated()
		if onUpdate != nil {
			onUpdate(*status)
		}
	}
	return nil
}

// moveTableDirToFailed moves a table directory from migrating to failed, preserving relative path.
func moveTableDirToFailed(dirPath string) {
	migratingRoot := config.GlobalWorkspaceProfile.GetMigratingDir()
	failedRoot := config.GlobalWorkspaceProfile.GetMigrationFailedDir()
	rel, err := filepath.Rel(migratingRoot, dirPath)
	if err != nil {
		return
	}
	destDir := filepath.Join(failedRoot, rel)
	_ = os.MkdirAll(filepath.Dir(destDir), 0755)
	_ = utils.MoveDirContents(dirPath, destDir)
	_ = os.Remove(dirPath)
}

// onSuccessful handles success outcome: cleans migrating db, prunes empty dirs, prints summary
func onSuccessful(ctx context.Context, status *MigrationStatus) error {
	// Remove any leftover db in migrating
	_ = os.Remove(filepath.Join(config.GlobalWorkspaceProfile.GetMigratingDir(), "tailpipe.db"))
	// Prune empty dirs in migrating
	if err := filepaths.PruneTree(config.GlobalWorkspaceProfile.GetMigratingDir()); err != nil {
		return fmt.Errorf("failed to prune empty directories in migrating: %w", err)
	}
	status.Finish("SUCCESS")
	status.StatusMessage()
	return nil
}

// onCancelled handles cancellation outcome: keep migrating db, prune empties, print summary
func onCancelled(ctx context.Context, status *MigrationStatus) error {
	// Do not move db; just prune empties so tree is clean
	_ = filepaths.PruneTree(config.GlobalWorkspaceProfile.GetMigratingDir())
	status.Finish("CANCELLED")
	status.StatusMessage()
	return nil
}

// onFailed handles failure outcome: move db to failed, prune empties, print summary
func onFailed(ctx context.Context, status *MigrationStatus) error {
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
	status.Finish("INCOMPLETE")
	status.StatusMessage()
	return nil
}
