package parquet

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/turbot/pipe-fittings/v2/backend"
	"github.com/turbot/tailpipe/internal/database"
)

const (
	// maxCompactionRowsPerChunk is the maximum number of rows to compact in a single insert operation
	maxCompactionRowsPerChunk = 5_000_000
)

func CompactDataFiles(ctx context.Context, db *database.DuckDb, updateFunc func(CompactionStatus), patterns ...PartitionPattern) error {
	slog.Info("Compacting DuckLake data files")

	t := time.Now()

	// get a list of partition key combinations which match any of the patterns
	partitionKeys, err := getPartitionKeysMatchingPattern(ctx, db, patterns)
	if err != nil {
		return fmt.Errorf("failed to get partition keys requiring compaction: %w", err)
	}

	if len(partitionKeys) == 0 {
		slog.Info("No matching partitions found for compaction")
		return nil
	}

	status, err := orderDataFiles(ctx, db, updateFunc, partitionKeys)
	if err != nil {
		slog.Error("Failed to compact DuckLake parquet files", "error", err)
		return err
	}

	// status.Uncompacted = uncompacted

	slog.Info("Expiring old DuckLake snapshots")
	// now expire unused snapshots
	if err := expirePrevSnapshots(ctx, db); err != nil {
		slog.Error("Failed to expire previous DuckLake snapshots", "error", err)
		return err
	}

	slog.Info("[SKIPPING] Merging adjacent DuckLake parquet files")
	// TODO merge_adjacent_files sometimes crashes, awaiting fix from DuckDb https://github.com/turbot/tailpipe/issues/530
	// so we should now have multiple, time ordered parquet files
	// now merge the the parquet files in the duckdb database
	// the will minimise the parquet file count to the optimum
	// if err := mergeParquetFiles(ctx, db); err != nil {
	//	slog.Error("Failed to merge DuckLake parquet files", "error", err)
	//	return nil, err
	// }

	slog.Info("Cleaning up expired files in DuckLake")
	// delete unused files
	if err := cleanupExpiredFiles(ctx, db); err != nil {
		slog.Error("Failed to cleanup expired files", "error", err)
		return err
	}

	// get the file count after merging and cleanup
	finalFileCount, err := getFileCountForPartitionKeys(ctx, db, partitionKeys)
	if err != nil {
		return err
	}
	// update status
	status.FinalFiles = finalFileCount
	// set the compaction time
	status.Duration = time.Since(t)

	// call final update
	updateFunc(*status)

	slog.Info("DuckLake compaction complete", "source_file_count", status.InitialFiles, "destination_file_count", status.FinalFiles)
	return nil
}

//nolint: unused // TODO merge_adjacent_files sometimes crashes, awaiting fix from DuckDb https://github.com/turbot/tailpipe/issues/530
// mergeParquetFiles combines adjacent parquet files in the DuckDB database.
func mergeParquetFiles(ctx context.Context, db *database.DuckDb) error {
	if _, err := db.ExecContext(ctx, "call merge_adjacent_files()"); err != nil {
		if ctx.Err() != nil {
			return err
		}
		return fmt.Errorf("failed to merge parquet files: %w", err)
	}
	return nil
}

// we order data files as follows:
// - get list of partition keys matching patterns. For each key:
//   - analyze file fragmentation to identify overlapping time ranges
//   - for each overlapping time range, reorder all data in that range
//   - delete original unordered entries for that time range
func orderDataFiles(ctx context.Context, db *database.DuckDb, updateFunc func(CompactionStatus), partitionKeys []*partitionKey) (*CompactionStatus, error) {
	slog.Info("Ordering DuckLake data files")

	status := NewCompactionStatus()
	// get total file and row count for status - iterating over partition keys
	for _, pk := range partitionKeys {
		status.InitialFiles += pk.fileCount
		status.TotalRows += pk.stats.rowCount
	}

	// Process each partition
	for _, pk := range partitionKeys {
		// determine which files are not time ordered
		unorderedRanges, err := getUnorderedRangesForPartitionKey(ctx, db, pk)
		if err != nil {
			slog.Error("failed to get unorderedRanges", "partition", pk, "error", err)
			return nil, err
		}
		slog.Debug("Partition key unorderedRanges",
			"tp_table", pk.tpTable,
			"tp_partition", pk.tpPartition,
			"tp_index", pk.tpIndex,
			"year", pk.year,
			"month", pk.month,

			"overlapping sets", len(unorderedRanges),
		)
		// if no files out of order, nothing to do
		if len(unorderedRanges) == 0 {
			slog.Info("Partition key is not fragmented - skipping compaction",
				"tp_table", pk.tpTable,
				"tp_partition", pk.tpPartition,
				"tp_index", pk.tpIndex,
				"year", pk.year,
				"month", pk.month,
				"file_count", pk.fileCount,
			)
			continue
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			// This is a system failure - stop everything
			return nil, fmt.Errorf("failed to begin transaction for partition %v: %w", pk, err)
		}

		slog.Info("Compacting partition entries",
			"tp_table", pk.tpTable,
			"tp_partition", pk.tpPartition,
			"tp_index", pk.tpIndex,
			"year", pk.year,
			"month", pk.month,
			"file_count", pk.fileCount,
		)

		// func to update status with number of rows compacted for this partition key
		// - passed to compactAndOrderPartitionKeyEntries
		updateRowsFunc := func(rowsCompacted int64) {
			status.RowsCompacted += rowsCompacted
			if status.TotalRows > 0 {
				status.ProgressPercent = (float64(status.RowsCompacted) / float64(status.TotalRows)) * 100
			}
			updateFunc(*status)
		}

		if err := compactAndOrderPartitionKeyEntries(ctx, tx, pk, unorderedRanges, updateRowsFunc); err != nil {
			slog.Error("failed to compact partition", "partition", pk, "error", err)
			txErr := tx.Rollback()
			if txErr != nil {
				slog.Error("failed to rollback transaction after compaction", "partition", pk, "error", txErr)
			}
			return nil, err
		}

		if err := tx.Commit(); err != nil {
			slog.Error("failed to commit transaction after compaction", "partition", pk, "error", err)
			txErr := tx.Rollback()
			if txErr != nil {
				slog.Error("failed to rollback transaction after compaction", "partition", pk, "error", txErr)
			}
			return nil, err
		}

		slog.Info("Compacted and ordered all partition entries",
			"tp_table", pk.tpTable,
			"tp_partition", pk.tpPartition,
			"tp_index", pk.tpIndex,
			"year", pk.year,
			"month", pk.month,
			"input_files", pk.fileCount,
		)

	}

	slog.Info("Finished ordering DuckLake data file")
	return status, nil
}

// compactAndOrderPartitionKeyEntries processes overlapping time ranges for a partition key:
// - iterates over each unordered time range
// - reorders all data within each time range (potentially in chunks for large ranges)
// - deletes original unordered entries for that time range
func compactAndOrderPartitionKeyEntries(ctx context.Context, tx *sql.Tx, pk *partitionKey, unorderedRanges []unorderedDataTimeRange, updateRowsCompactedFunc func(int64)) error {

	slog.Debug("partition statistics",
		"tp_table", pk.tpTable,
		"tp_partition", pk.tpPartition,
		"tp_index", pk.tpIndex,
		"year", pk.year,
		"month", pk.month,
		"row_count", pk.stats.rowCount,
		"total file_count", pk.fileCount,
		"min_timestamp", pk.stats.minTimestamp,
		"max_timestamp", pk.stats.maxTimestamp,
		"total_ranges", len(unorderedRanges),
	)

	// Process each overlapping time range
	for i, timeRange := range unorderedRanges {
		slog.Debug("processing overlapping time range",
			"range_index", i+1,
			"start_time", timeRange.StartTime,
			"end_time", timeRange.EndTime,
			"row_count", timeRange.RowCount)

		// Use the pre-calculated time range and row count from the struct
		minTime := timeRange.StartTime
		maxTime := timeRange.EndTime
		rowCount := timeRange.RowCount

		// Determine chunking strategy for this time range
		chunks, intervalDuration := determineChunkingInterval(minTime, maxTime, rowCount)

		slog.Debug("processing time range in chunks",
			"range_index", i+1,
			"row_count", rowCount,
			"chunks", chunks,
			"interval_duration", intervalDuration.String())

		// Process this time range in chunks
		currentStart := minTime
		for i := 1; currentStart.Before(maxTime); i++ {
			currentEnd := currentStart.Add(intervalDuration)
			if currentEnd.After(maxTime) {
				currentEnd = maxTime
			}

			// For the final chunk, make it inclusive to catch the last row
			isFinalChunk := currentEnd.Equal(maxTime)

			rowsInserted, err := insertOrderedDataForTimeRange(ctx, tx, pk, currentStart, currentEnd, isFinalChunk)
			if err != nil {
				return fmt.Errorf("failed to insert ordered data for time range %s to %s: %w",
					currentStart.Format("2006-01-02 15:04:05"),
					currentEnd.Format("2006-01-02 15:04:05"), err)
			}
			updateRowsCompactedFunc(rowsInserted)
			slog.Debug(fmt.Sprintf("processed chunk %d/%d for range %d", i, chunks, i+1))

			// Ensure next chunk starts exactly where this one ended to prevent gaps
			currentStart = currentEnd
		}

		// Delete original unordered entries for this time range
		err := deleteUnorderedEntriesForTimeRange(ctx, tx, pk, minTime, maxTime)
		if err != nil {
			return fmt.Errorf("failed to delete unordered entries for time range: %w", err)
		}

		slog.Debug("completed time range",
			"range_index", i+1)
	}

	return nil
}

// insertOrderedDataForTimeRange inserts ordered data for a specific time range within a partition key
func insertOrderedDataForTimeRange(ctx context.Context, tx *sql.Tx, pk *partitionKey, startTime, endTime time.Time, isFinalChunk bool) (int64, error) {
	// For the final chunk, use inclusive end time to catch the last row
	timeEndOperator := "<"
	if isFinalChunk {
		timeEndOperator = "<="
	}

	// For overlapping files, we need to reorder ALL rows in the overlapping time range
	// Since files overlap, we can't distinguish which specific rows came from which files
	// So we reorder all rows in the time range for this partition
	args := []interface{}{startTime, endTime, pk.tpPartition, pk.tpIndex}

	tableName, err := backend.SanitizeDuckDBIdentifier(pk.tpTable)
	if err != nil {
		return 0, err
	}
	//nolint: gosec // sanitized
	insertQuery := fmt.Sprintf(`insert into %s 
		select * from %s 
		where tp_timestamp >= ?
		  and tp_timestamp %s ?
		  and tp_partition = ?
		  and tp_index = ?
		order by tp_timestamp`,
		tableName,
		tableName,
		timeEndOperator)

	result, err := tx.ExecContext(ctx, insertQuery, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to insert ordered data for time range: %w", err)
	}
	rowsInserted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected count: %w", err)
	}
	return rowsInserted, nil
}

// deleteUnorderedEntriesForTimeRange deletes the original unordered entries for a specific time range within a partition key
func deleteUnorderedEntriesForTimeRange(ctx context.Context, tx *sql.Tx, pk *partitionKey, startTime, endTime time.Time) error {
	// Delete all rows in the time range for this partition key (we're re-inserting them in order)
	tableName, err := backend.SanitizeDuckDBIdentifier(pk.tpTable)
	if err != nil {
		return err
	}
	//nolint: gosec // sanitized
	deleteQuery := fmt.Sprintf(`delete from %s 
		where tp_partition = ?
		  and tp_index = ?
		  and tp_timestamp >= ?
		  and tp_timestamp <= ?`,
		tableName)

	args := []interface{}{pk.tpPartition, pk.tpIndex, startTime, endTime}

	_, err = tx.ExecContext(ctx, deleteQuery, args...)
	if err != nil {
		return fmt.Errorf("failed to delete unordered entries for time range: %w", err)
	}

	return nil
}

// determineChunkingInterval calculates the optimal chunking strategy for a time range based on row count.
// It returns the number of chunks and the duration of each chunk interval.
// For large datasets, it splits the time range into multiple chunks to stay within maxCompactionRowsPerChunk.
// Ensures minimum chunk interval is at least 1 hour to avoid excessive fragmentation.
func determineChunkingInterval(startTime, endTime time.Time, rowCount int64) (chunks int, intervalDuration time.Duration) {
	intervalDuration = endTime.Sub(startTime)
	chunks = 1

	// If row count is greater than maxCompactionRowsPerChunk, calculate appropriate chunk interval
	if rowCount > maxCompactionRowsPerChunk {
		chunks = int((rowCount + maxCompactionRowsPerChunk - 1) / maxCompactionRowsPerChunk)
		intervalDuration = intervalDuration / time.Duration(chunks)

		// Ensure minimum interval is at least 1 hour
		if intervalDuration < time.Hour {
			intervalDuration = time.Hour
		}
	}

	return chunks, intervalDuration
}
