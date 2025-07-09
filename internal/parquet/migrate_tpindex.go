package parquet

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	sdkconstants "github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
)

const (
	sourceFileColumnName = "__duckdb_source_file"
	migrateTempTableName = "_raw_tp_data"
)

func migrateTpIndex(ctx context.Context, db *database.DuckDb, baseDir string, updateFunc func(CompactionStatus), patterns []PartitionPattern) error {
	fileRootProvider := &FileRootProvider{}
	for _, partition := range config.GlobalConfig.Partitions {
		if PartitionMatchesPatterns(partition.TableName, partition.ShortName, patterns) {
			err := migrateTpIndexForPartition(ctx, db, baseDir, partition, fileRootProvider, updateFunc)
			if err != nil {
				if ctx.Err() != nil {
					return err
				}
				return fmt.Errorf("failed to migrate tp_index for partition %s: %w", partition.UnqualifiedName, err)
			} else {
				slog.Info("Migrated tp_index files for partition", "partition", partition.UnqualifiedName, "index_expression", partition.TpIndexColumn)
			}
		}
	}
	return nil
}

func migrateTpIndexForPartition(ctx context.Context, db *database.DuckDb, baseDir string, partition *config.Partition, fileRootProvider *FileRootProvider, updateFunc func(CompactionStatus)) error {

	// executeMigrationQuery runs the DuckDB query to migrate the tp_index files for a given partition.
	// it read the partition data into a temporary table, then writes the data to with the migrated tp_index
	// to intermediate the output files (with extension .tmp) and returns the list of output files.
	outputFiles, err := executeMigrationQuery(ctx, db, baseDir, partition, fileRootProvider)
	if err != nil {
		return err
	}
	if len(outputFiles) == 0 {
		return nil // nothing to migrate
	}

	// read the source files from the temporary table
	sourceFiles, err := readSourceFiles(ctx, db)
	if err != nil {
		return err
	}

	// now rename the source files to add a .migrated extension
	renamedSourceFiles, err := addExtensionToFiles(sourceFiles, ".migrated")
	if err != nil {
		if err := deleteFilesConcurrently(ctx, outputFiles, baseDir); err != nil {
			slog.Error("Failed to delete temp files after migration failure", "error", err)
		}
		return err
	}

	// rename the output files to remove the .tmp extension
	if err := removeExtensionFromFiles(outputFiles, ".tmp"); err != nil {
		if err := deleteFilesConcurrently(ctx, outputFiles, baseDir); err != nil {
			slog.Error("Failed to delete temp files after migration failure", "error", err)
		}

		if err := removeExtensionFromFiles(renamedSourceFiles, ".migrated"); err != nil {
			slog.Error("Failed to rename source files back to original names after migration failure", "error", err)
		}
		return err
	}

	// finally, delete the renamed source parquet files
	if err := deleteFilesConcurrently(ctx, renamedSourceFiles, baseDir); err != nil {
		slog.Error("Failed to delete renamed source parquet files after migration", "error", err)
	}

	status := CompactionStatus{
		MigrateSource: len(sourceFiles),
		MigrateDest:   len(outputFiles),
		PartitionIndexExpressions: map[string]string{
			partition.UnqualifiedName: partition.TpIndexColumn,
		},
	}
	updateFunc(status)

	return nil
}

// executeMigrationQuery runs the DuckDB query to migrate the tp_index files for a given partition.
// It reads the partition data into a temporary table, writes the data with the migrated tp_index
// to intermediate output files (with .tmp extension), and returns the list of output file paths.
func executeMigrationQuery(ctx context.Context, db *database.DuckDb, baseDir string, partition *config.Partition, fileRootProvider *FileRootProvider) ([]string, error) {
	// TODO #DL this is out of date/not needed
	// Get the file glob pattern for all files in this partition
	fileGlob := "" //filepaths.GetParquetFileGlobForPartition(baseDir, partition.TableName, partition.ShortName, "")

	// get unique file root to use for the output files
	fileRoot := fileRootProvider.GetFileRoot()
	// columns to partition by
	partitionColumns := []string{sdkconstants.TpTable, sdkconstants.TpPartition, sdkconstants.TpIndex, sdkconstants.TpDate}

	// build the query to read the parquet files into a temporary table
	query := fmt.Sprintf(`
create or replace temp table %s as
select
    *,
    %s,
from read_parquet('%s', filename=%s);

copy (
    select
        * exclude (tp_index, %s),
        %s as tp_index
    from %s
) to '%s' (
    format parquet,
    partition_by (%s),
    return_files true,
    overwrite_or_ignore,
    filename_pattern '%s_{i}',
    file_extension 'parquet.tmp'
);
`,
		migrateTempTableName,                // e.g. "_raw_tp_data"
		sourceFileColumnName,                // select filename
		fileGlob,                            // parquet file glob path
		sourceFileColumnName,                // read filename column from parquet
		sourceFileColumnName,                // exclude source file column from the copy
		partition.TpIndexColumn,             // replacement tp_index expression
		migrateTempTableName,                // again used in the copy
		baseDir,                             // output path
		strings.Join(partitionColumns, ","), // partition columns
		fileRoot,                            // filename root prefix
	)

	var rowCount int64
	var outputFilesRaw []interface{}
	err := db.QueryRowContext(ctx, query).Scan(&rowCount, &outputFilesRaw)
	if err != nil {
		// if this is a no files found error, we can ignore it
		if strings.Contains(err.Error(), "No files found") {
			slog.Info("No files found for migration", "partition", partition.UnqualifiedName)
			return nil, nil
		}
		return nil, fmt.Errorf("failed to scan return_files output: %w", err)
	}

	outputFiles := make([]string, len(outputFilesRaw))
	for i, val := range outputFilesRaw {
		if str, ok := val.(string); ok {
			outputFiles[i] = str
		} else {
			return nil, fmt.Errorf("unexpected file path type %T at index %d", val, i)
		}
	}

	return outputFiles, nil
}

// readSourceFiles reads the source files column from the temporary table created during the tp_index migration.
func readSourceFiles(ctx context.Context, db *database.DuckDb) ([]string, error) {
	query := fmt.Sprintf(`select distinct %s from %s`, sourceFileColumnName, migrateTempTableName)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to read source files from temp table: %w", err)
	}
	defer rows.Close()

	var sourceFiles []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, fmt.Errorf("failed to scan source file path: %w", err)
		}
		sourceFiles = append(sourceFiles, path)
	}
	return sourceFiles, nil
}
