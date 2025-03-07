package parquet

import (
	"fmt"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/repository"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/filepaths"
)

func DeleteParquetFiles(partition *config.Partition, from time.Time) (int, error) {
	db, err := database.NewDuckDb()
	if err != nil {
		return 0, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer db.Close()

	dataDir := config.GlobalWorkspaceProfile.GetDataDir()
	var rowCount int

	// load the partition state
	partitionState, err := repository.GetPartitionState(partition.ShortName)
	if err != nil {
		return 0, err
	}
	// if the partition has an InvalidFromDate, claan up invalid and temp files from latest of:
	// -  InvalidFromDate
	// - delete from time
	invalidDeleteFromTime := partitionState.InvalidFromDate
	if !invalidDeleteFromTime.IsZero() {
		if from.IsZero() {
			from = invalidDeleteFromTime
		} else if invalidDeleteFromTime.After(from) {
			from = invalidDeleteFromTime
		}
	}

	if from.IsZero() {
		// if there is no from time, delete the entire partition folder
		rowCount, err = deletePartition(db, dataDir, partition)
	} else {
		// otherwise delete partition data for a time range
		rowCount, err = deletePartitionFrom(db, dataDir, partition, from)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to delete partition: %w", err)
	}

	// delete all empty folders underneath the partition folder
	partitionDir := filepaths.GetParquetPartitionPath(dataDir, partition.TableName, partition.ShortName)
	pruneErr := filepaths.PruneTree(partitionDir)
	if pruneErr != nil {
		// do not return error - just log
		slog.Warn("DeleteParquetFiles failed to prune empty folders", "error", pruneErr)
	}

	return rowCount, nil
}

func deletePartitionFrom(db *database.DuckDb, dataDir string, partition *config.Partition, from time.Time) (_ int, err error) {
	parquetGlobPath := filepaths.GetParquetFileGlobForPartition(dataDir, partition.TableName, partition.ShortName, "")

	//nolint:gosec // we cannot use params inside read_parquet - and this is a trusted source
	query := fmt.Sprintf(`
    SELECT 
    DISTINCT '%s/tp_table=' || tp_table || '/tp_partition=' || tp_partition || '/tp_index=' || tp_index || '/tp_date=' || tp_date AS hive_path,
	COUNT(*) OVER() AS total_files
    FROM read_parquet('%s', hive_partitioning=true)
    WHERE tp_partition = ?
 	AND tp_date >= ?`,
		dataDir, parquetGlobPath)

	rows, err := db.Query(query, partition.ShortName, from)
	if err != nil {
		// is this an error because there are no files?
		if isNoFilesFoundError(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to query parquet folder names: %w", err)
	}
	defer rows.Close()

	var folders []string
	var count int
	// Iterate over the results
	for rows.Next() {
		var folder string
		if err := rows.Scan(&folder, &count); err != nil {
			return 0, fmt.Errorf("failed to scan parquet folder name: %w", err)
		}
		folders = append(folders, folder)
	}

	var errors = make(map[string]error)
	for _, folder := range folders {
		if err := os.RemoveAll(folder); err != nil {
			errors[folder] = err
		}
	}

	return len(folders), nil
}

func deletePartition(db *database.DuckDb, dataDir string, partition *config.Partition) (int, error) {
	parquetGlobPath := filepaths.GetParquetFileGlobForPartition(dataDir, partition.TableName, partition.ShortName, "")

	// get count of parquet files
	//nolint:gosec// have to build string for read_parquet
	query := fmt.Sprintf(`
		SELECT COUNT(DISTINCT filename)
		FROM read_parquet('%s', hive_partitioning=true, filename=true)
		WHERE tp_partition = ?
	`, parquetGlobPath)

	// Execute the query with a parameter for the tp_partition filter
	q := db.QueryRow(query, partition.ShortName)
	// read the result
	var count int
	err := q.Scan(&count)
	if err != nil && !isNoFilesFoundError(err) {
		return 0, fmt.Errorf("failed to query parquet file count: %w", err)
	}

	partitionFolder := filepaths.GetParquetPartitionPath(dataDir, partition.TableName, partition.ShortName)
	err = os.RemoveAll(partitionFolder)
	if err != nil {
		return 0, fmt.Errorf("failed to delete partition folder: %w", err)
	}
	return count, nil
}

func isNoFilesFoundError(err error) bool {
	return strings.HasPrefix(err.Error(), "IO Error: No files found")
}
