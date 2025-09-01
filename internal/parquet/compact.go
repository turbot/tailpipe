package parquet

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/turbot/tailpipe/internal/database"
)

const (
	// maxCompactionRowsPerChunk is the maximum number of rows to compact in a single insert operation
	maxCompactionRowsPerChunk = 1_000_000
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

	//status.Uncompacted = uncompacted

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
	//if err := mergeParquetFiles(ctx, db); err != nil {
	//	slog.Error("Failed to merge DuckLake parquet files", "error", err)
	//	return nil, err
	//}

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
// - get list of partition keys matching patterns. For each  key:
//   - order entries <potentially split into day chunks>:
//   - get max row id of rows with that partition key
//   - reinsert ordered data for partition key
//   - dedupe: delete rows for partition key with rowid <= prev max row id
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
		// TODO #compact determine how fragmented this partition key is and only order if needed (unless 'force' is set?)
		metrics, err := pk.getDisorderMetrics(ctx, db)
		if err != nil {
			slog.Error("failed to get disorder metrics", "partition", pk, "error", err)
			return nil, err
		}
		slog.Debug("Partition key disorder metrics",
			"tp_table", pk.tpTable,
			"tp_partition", pk.tpPartition,
			"tp_index", pk.tpIndex,
			"year", pk.year,
			"month", pk.month,
			"total files", metrics.totalFiles,
			"overlapping files", metrics.overlappingFiles,
		)
		if len(metrics.overlappingFiles) == 0 {
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

		//if not_fragmented
		//	continue
		//}

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

		if err := compactAndOrderPartitionKeyEntries(ctx, tx, pk, updateRowsFunc); err != nil {
			slog.Error("failed to compact partition", "partition", pk, "error", err)
			tx.Rollback()
			return nil, err
		}

		if err := tx.Commit(); err != nil {
			slog.Error("failed to commit transaction after compaction", "partition", pk, "error", err)
			tx.Rollback()
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

//	we order data files as follows:
//
// - get the row count, time range and max row id for the partition key
// - determine a time interval which will give us row counts <= maxCompactionRowsPerChunk
// - loop over time intervals. For each interval
//   - reinsert ordered data for partition key
//   - dedupe: delete rows for partition key with rowid <= prev max row id
func compactAndOrderPartitionKeyEntries(ctx context.Context, tx *sql.Tx, pk *partitionKey, updateRowsCompactedFunc func(int64)) error {

	slog.Debug("partition statistics",
		"tp_table", pk.tpTable,
		"tp_partition", pk.tpPartition,
		"tp_index", pk.tpIndex,
		"year", pk.year,
		"month", pk.month,
		"row_count", pk.stats.rowCount,
		"file_count", pk.fileCount,
		"max_rowid", pk.stats.maxRowId,
		"min_timestamp", pk.stats.minTimestamp,
		"max_timestamp", pk.stats.maxTimestamp,
	)

	intervalDuration := pk.stats.maxTimestamp.Sub(pk.stats.minTimestamp)
	chunks := 1

	// If row count is greater than maxCompactionRowsPerChunk, calculate appropriate chunk interval
	if pk.stats.rowCount > maxCompactionRowsPerChunk {
		// Calculate time interval to get approximately maxCompactionRowsPerChunk rows per chunk
		// Use hour-based intervals for more granular control
		chunks = int((pk.stats.rowCount + maxCompactionRowsPerChunk - 1) / maxCompactionRowsPerChunk) // Ceiling division
		intervalDuration = intervalDuration / time.Duration(chunks)

		// Ensure minimum interval is at least 1 hour
		if intervalDuration < time.Hour {
			intervalDuration = time.Hour
		}
	}

	slog.Debug("processing partition in chunks",
		"total_rows", pk.stats.rowCount,
		"chunks", chunks,
		"interval_duration", intervalDuration.String())

	// Process data in time-based chunks
	currentStart := pk.stats.minTimestamp
	i := 1
	for currentStart.Before(pk.stats.maxTimestamp) {
		currentEnd := currentStart.Add(intervalDuration)
		if currentEnd.After(pk.stats.maxTimestamp) {
			currentEnd = pk.stats.maxTimestamp
		}

		// For the final chunk, make it inclusive to catch the last row
		isFinalChunk := currentEnd.Equal(pk.stats.maxTimestamp)

		rowsInserted, err := insertOrderedDataForPartitionTimeRange(ctx, tx, pk, currentStart, currentEnd, isFinalChunk)
		if err != nil {
			return fmt.Errorf("failed to insert ordered data for time range %s to %s: %w",
				currentStart.Format("2006-01-02 15:04:05"),
				currentEnd.Format("2006-01-02 15:04:05"), err)
		}
		updateRowsCompactedFunc(rowsInserted)
		slog.Debug(fmt.Sprintf("processed chunk %d/%d", i, chunks))

		i++

		// Ensure next chunk starts exactly where this one ended to prevent gaps
		currentStart = currentEnd
	}

	slog.Debug("completed all time chunks for partition, deleting unordered entries",
		"tp_table", pk.tpTable,
		"tp_partition", pk.tpPartition,
		"tp_index", pk.tpIndex,
		"year", pk.year,
		"month", pk.month,
		"max_rowid", pk.stats.maxRowId)

	// we have sorted  and reinserted all data for this partition key - now delete all unordered entries (i.e. where rowid <  maxRowId)
	deleteQuery := fmt.Sprintf(`delete from "%s" 
		where tp_partition = ?
		  and tp_index = ?
		  and year(tp_timestamp) = ?
		  and month(tp_timestamp) = ?
		  and rowid <= ?`,
		pk.tpTable)

	_, err := tx.ExecContext(ctx, deleteQuery,
		pk.tpPartition,
		pk.tpIndex,
		pk.year,
		pk.month,
		pk.stats.maxRowId)
	if err != nil {
		return fmt.Errorf("failed to delete unordered data for partition: %w", err)
	}

	// Validate total rows processed matches original count
	finalCountQuery := fmt.Sprintf(`select count(*) from "%s" 
		where tp_partition = ?
		  and tp_index = ?
		  and year(tp_timestamp) = ?
		  and month(tp_timestamp) = ?`,
		pk.tpTable)

	var finalRowCount int64
	if err := tx.QueryRowContext(ctx, finalCountQuery,
		pk.tpPartition,
		pk.tpIndex,
		pk.year,
		pk.month).Scan(&finalRowCount); err != nil {
		return fmt.Errorf("failed to get final row count: %w", err)
	}

	if finalRowCount != pk.stats.rowCount {
		return fmt.Errorf("total row count mismatch: expected %d, got %d", pk.stats.rowCount, finalRowCount)
	}

	return nil
}

// insertOrderedDataForPartitionTimeRange inserts ordered data for a specific time range
func insertOrderedDataForPartitionTimeRange(ctx context.Context, tx *sql.Tx, pk *partitionKey, startTime, endTime time.Time, isFinalChunk bool) (int64, error) {
	// For the final chunk, use inclusive end time to catch the last row
	timeCondition := "tp_timestamp < ?"
	if isFinalChunk {
		timeCondition = "tp_timestamp <= ?"
	}

	insertQuery := fmt.Sprintf(`insert into "%s" 
		select * from "%s" 
		where tp_partition = ?
		  and tp_index = ?
		  and tp_timestamp >= ?
		  and %s
		order by tp_timestamp`,
		pk.tpTable,
		pk.tpTable,
		timeCondition)

	result, err := tx.ExecContext(ctx, insertQuery,
		pk.tpPartition,
		pk.tpIndex,
		startTime,
		endTime)
	if err != nil {
		return 0, fmt.Errorf("failed to insert ordered data for time range: %w", err)
	}
	rowsInserted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected count: %w", err)
	}
	return rowsInserted, nil
}

// SafeIdentifier ensures that SQL identifiers (like table or column names)
// are safely quoted using double quotes and escaped appropriately.
//
// For example:
//
//	input:  my_table         → output:  "my_table"
//	input:  some"col         → output:  "some""col"
//	input:  select           → output:  "select"    (reserved keyword)
//
// TODO move to pipe-helpers https://github.com/turbot/tailpipe/issues/517
func SafeIdentifier(identifier string) string {
	escaped := strings.ReplaceAll(identifier, `"`, `""`)
	return `"` + escaped + `"`
}

// EscapeLiteral safely escapes SQL string literals for use in WHERE clauses,
// INSERTs, etc. It wraps the string in single quotes and escapes any internal
// single quotes by doubling them.
//
// For example:
//
//	input:  O'Reilly         → output:  'O''Reilly'
//	input:  2025-08-01       → output:  '2025-08-01'
//
// TODO move to pipe-helpers https://github.com/turbot/tailpipe/issues/517
func EscapeLiteral(literal string) string {
	escaped := strings.ReplaceAll(literal, `'`, `''`)
	return `'` + escaped + `'`
}
