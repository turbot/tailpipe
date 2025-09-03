package migration

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/filepaths"
	"github.com/turbot/tailpipe/internal/parquet"
)

func MigrateDataToDucklake(ctx context.Context) error {
	// Determine source and migration directories
	dataDefaultDir := config.GlobalWorkspaceProfile.GetDataDir()
	migratingDefaultDir := config.GlobalWorkspaceProfile.GetMigratingDir()

	dataHasDb := hasTailpipeDb(dataDefaultDir)
	migratingHasDb := hasTailpipeDb(migratingDefaultDir)

	if !dataHasDb && !migratingHasDb {
		slog.Info("No migration needed - no tailpipe.db found in data or migrating directory")
		return nil
	}

	// Choose DB path for discovery
	var discoveryDbPath string
	if dataHasDb {
		discoveryDbPath = filepath.Join(dataDefaultDir, "tailpipe.db")
	} else {
		discoveryDbPath = filepath.Join(migratingDefaultDir, "tailpipe.db")
	}

	// STEP 1: Discover legacy tables and their schemas (from chosen DB)
	views, schemas, err := discoverLegacyTables(ctx, discoveryDbPath)
	if err != nil {
		return fmt.Errorf("failed to discover legacy tables: %w", err)
	}
	slog.Info("Views: ", "views", views)
	slog.Info("Schemas: ", "schemas", schemas)

	// STEP 2: If migration is needed (detected .db in data dir), move the whole contents of data dir
	// into ~/.tailpipe/migration/migrating respecting the same folder structure.
	if dataHasDb {
		if err := utils.MoveDirContents(dataDefaultDir, migratingDefaultDir); err != nil {
			return fmt.Errorf("failed to move data dir contents to migrating: %w", err)
		}
	}

	// Scan migrating/default for tp_table=*
	baseDir := migratingDefaultDir
	matchedTableDirs, unmatchedTableDirs, err := findMatchingTableDirs(baseDir, views)
	if err != nil {
		return err
	}
	slog.Info("Matched tp_table directories", "dirs", matchedTableDirs)
	for _, d := range unmatchedTableDirs {
		tname := strings.TrimPrefix(filepath.Base(d), "tp_table=")
		slog.Warn("Table %s has data but no view in database; moving without migration", "table", tname, "dir", d)
		if err := moveDirFromMigratingToMigrated(d); err != nil {
			return err
		}
	}

	// STEP 3: Traverse matched table directories, find leaf nodes with parquet files,
	// and perform INSERT within a transaction. On success, move leaf dir to migrated.
	db, err := database.NewDuckDb(database.WithDuckLakeEnabled(true))
	if err != nil {
		return err
	}
	defer db.Close()

	for _, tableDir := range matchedTableDirs {
		tableName := strings.TrimPrefix(filepath.Base(tableDir), "tp_table=")
		if tableName == "" {
			continue
		}
		ts := schemas[tableName]
		if err := migrateTableDirectory(ctx, db, tableName, tableDir, ts); err != nil {
			return err
		}
	}

	// After migrating all leaf nodes, move the legacy tailpipe.db file to migrated (if present)
	if err := moveLegacyDbFile(migratingDefaultDir); err != nil {
		return err
	}
	// Finally, prune any empty directories left in migrating/default
	if err := filepaths.PruneTree(migratingDefaultDir); err != nil {
		return fmt.Errorf("failed to prune empty directories in migrating: %w", err)
	}

	return nil
}

// discoverLegacyTables enumerates legacy DuckDB views and, for each view, its schema.
// It returns the list of view names and a map of view name to its schema (column->type).
// If the legacy database contains no views, both return values are empty.
func discoverLegacyTables(ctx context.Context, dbPath string) ([]string, map[string]*schema.TableSchema, error) {
	views, err := database.GetLegacyTableViews(ctx, dbPath)
	if err != nil || len(views) == 0 {
		return []string{}, map[string]*schema.TableSchema{}, err
	}

	schemas := make(map[string]*schema.TableSchema)
	for _, v := range views {
		ts, err := database.GetLegacyTableViewSchema(ctx, v, dbPath)
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
	err := parquet.EnsureDuckLakeTable(ts.Columns, db, tableName)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}

	var parquetFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			subDir := filepath.Join(dirPath, entry.Name())
			if err := migrateTableDirectory(ctx, db, tableName, subDir, ts); err != nil {
				return err
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
			_ = tx.Rollback()
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}

		// On success, move the entire leaf directory from migrating to migrated
		migratingRoot := config.GlobalWorkspaceProfile.GetMigratingDir()
		migratedRoot := config.GlobalWorkspaceProfile.GetMigratedDir()
		destDir := strings.Replace(dirPath, migratingRoot, migratedRoot, 1)
		if err := os.MkdirAll(filepath.Dir(destDir), 0755); err != nil {
			return err
		}
		if err := os.Rename(dirPath, destDir); err != nil {
			return err
		}
		slog.Info("Migrated leaf node", "table", tableName, "source", dirPath, "destination", destDir)
	}

	return nil
}

// moveDirFromMigratingToMigrated moves a directory tree from the migrating root to the migrated root.
func moveDirFromMigratingToMigrated(srcPath string) error {
	migratingRoot := config.GlobalWorkspaceProfile.GetMigratingDir()
	migratedRoot := config.GlobalWorkspaceProfile.GetMigratedDir()
	if !strings.HasPrefix(srcPath, migratingRoot) {
		return fmt.Errorf("path %s is not within migrating root %s", srcPath, migratingRoot)
	}
	rel, err := filepath.Rel(migratingRoot, srcPath)
	if err != nil {
		return err
	}
	destPath := filepath.Join(migratedRoot, rel)
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return err
	}
	return os.Rename(srcPath, destPath)
}
