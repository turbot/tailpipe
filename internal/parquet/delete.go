package parquet

import (
	"fmt"
	"github.com/turbot/pipe-fittings/v2/utils"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/repository"

	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/filepaths"
)

func DeleteParquetFiles(partition *config.Partition, from time.Time) (rowCount int, err error) {
	db, err := database.NewDuckDb()
	if err != nil {
		return 0, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer db.Close()

	dataDir := config.GlobalWorkspaceProfile.GetDataDir()

	// load the partition state
	rs, err := repository.Load()
	if err != nil {
		return 0, err
	}
	partitionState := rs.GetPartitionState(partition.UnqualifiedName)

	// handle invalid state and determine if we should save state changes
	if partitionState.State == repository.CollectionStateInvalid {
		slog.Info("DeleteParquetFiles - partition is in invalid state", "partition", partition.UnqualifiedName)
		JUST ALWAYS CLEAR IN COMPACT
		if shouldClearInvalidState(partitionState.InvalidFromDate, from) {
			// clear the invalid state
			partitionState.SetIdle()
			// defer saving state - only save if there is no error
			defer func() {
				if err == nil {
					slog.Info("DeleteParquetFiles - saving partition with idle state", "partition", partition.UnqualifiedName)
					rs.SetPartitionState(partition.UnqualifiedName, partitionState)
					if saveErr := rs.Save(); saveErr != nil {
						slog.Error("failed to save partition state", "error", saveErr)
					}
				}
			}()
		}

		// now figure out what date to delete temp/invalid files from - the later of the from date and the InvalidFromDate
		deleteInvalidDate := getDeleteInvalidDate(from, partitionState.InvalidFromDate)

		// delete invalid and temp files from the calculated date
		if err := deleteInvalidParquetFiles(dataDir, partition, deleteInvalidDate); err != nil {
			// do not fail if we cannot delete invalid files - just log the error
			slog.Warn("DeleteParquetFiles failed to delete all invalid parquet files", "error", err)
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

// shouldClearInvalidState handles the case where a partition is in an invalid state.
// It determines if we should clean up invalid data and if we can reset the invalid state after the partition deletion
// Returns an updated partition state and a flag indicating whether we should save state changes
func shouldClearInvalidState(invalidFromData, from time.Time) bool {
	// if there is no invalid from date, we can only clear invalid state if the from time is also zero
	if invalidFromData.IsZero() {
		res := from.IsZero()
		if res {
			slog.Info("shouldClearInvalidState - no invalidFromDate, and no from date (meaning we are clearing full partition) so clearing invalid state")
		} else {
			slog.Info("shouldClearInvalidState - no invalidFromDate, but 'from' date is set (meaning we are NOT clearing full partition) - so NOT clearing invalid state")
		}

		return res
	}

	// if the from time is zero or invalidFromDate is equal or later than the from time, we can clear the invalid state
	res := from.IsZero() || invalidFromData.Compare(from) <= 0
	slog.Info("shouldClearInvalidState", "invalidFromDate", invalidFromData, "deleting from", from, "shouldClearInvalidState", res)
	return res
}

// getDeleteInvalidDate determines the date from which to delete invalid files
// It returns the later of the from date and the InvalidFromDate
func getDeleteInvalidDate(from, invalidFromDate time.Time) time.Time {
	deleteInvalidDate := from
	if invalidFromDate.After(from) {
		deleteInvalidDate = invalidFromDate
	}
	return deleteInvalidDate
}

// deleteInvalidParquetFiles deletes invalid and temporary parquet files for a partition from a given date
func deleteInvalidParquetFiles(dataDir string, partition *config.Partition, from time.Time) error {
	slog.Info("deleteInvalidParquetFiles - deleting invalid parquet files", "partition", partition.UnqualifiedName, "delete from", from)

	// get glob patterns for invalid and temp files
	invalidGlob := filepaths.GetTempAndInvalidParquetFileGlobForPartition(dataDir, partition.TableName, partition.ShortName)

	// find all matching files
	filesToDelete, err := filepath.Glob(invalidGlob)
	if err != nil {
		return fmt.Errorf("failed to find invalid files: %w", err)
	}

	slog.Info("deleteInvalidParquetFiles", "invalid count", len(filesToDelete), "files", filesToDelete)
	var failures int

	// delete each file
	for _, file := range filesToDelete {
		if !from.IsZero() {
			// extract date from file path
			fields, err := filepaths.ExtractPartitionFields(file)
			if err != nil {
				// if we can't parse the date, skip this file
				continue
			}

			// only delete files from or after the specified date
			if fields.Date.IsZero() || fields.Date.Before(from) {
				continue
			}
		}

		if err := os.Remove(file); err != nil {
			slog.Debug("failed to delete invalid parquet file", "file", file, "error", err)
			failures++
		}
	}

	if failures > 0 {
		return fmt.Errorf("failed to delete %d invalid parquet %s", failures, utils.Pluralize("file", failures))
	}
	return nil
}
