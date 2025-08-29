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

// we order data files as follows:
// - get list of partition keys matching patterns. For each  key:
//   - order entries <potentially split into day chunks>:
//   - get max row id of rows with that partition key
//   - reinsert ordered data for partition key
//   - dedupe: delete rows for partition key with rowid <= prev max row id
func orderDataFiles(ctx context.Context, db *database.DuckDb, updateFunc func(*CompactionStatus), patterns []PartitionPattern) (*CompactionStatus, error) {
	slog.Info("Ordering DuckLake data files")

	status := NewCompactionStatus()

	// get a list of partition key combinations which match any of the patterns
	partitionKeys, err := getPartitionKeysMatchingPattern(ctx, db, patterns)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition keys requiring compaction: %w", err)
	}

	if len(partitionKeys) == 0 {
		slog.Info("No matching partitions found for compaction")
		return nil, nil
	}
	// get total file count for status - iterating over partition keys
	for _, pk := range partitionKeys {
		status.InitialFiles += pk.fileCount
	}

	// first we want to identify how may files and rows in total we need to compact
	rowCounts := make([]*partitionKeyRows, len(partitionKeys))
	for i, pk := range partitionKeys {
		pkr, err := getPartitionKeyRowCount(ctx, db, pk)
		if err != nil {
			return nil, fmt.Errorf("failed to get row count for partition key %v: %w", pk, err)
		}
		rowCounts[i] = pkr
	}

	// Process each partition
	for i, partitionKey := range partitionKeys {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			// This is a system failure - stop everything
			return nil, fmt.Errorf("failed to begin transaction for partition %v: %w", partitionKey, err)
		}

		// TODO #compact determine how fragmented this partition key is and only order if needed (unless 'force' is set?)
		//if not_fragmented
		//	continue
		//}

		slog.Info("Compacting partition entries",
			"tp_table", partitionKey.tpTable,
			"tp_partition", partitionKey.tpPartition,
			"tp_index", partitionKey.tpIndex,
			"year", partitionKey.year,
			"month", partitionKey.month,
			"file_count", partitionKey.fileCount,
		)

		partitionRows := rowCounts[i]

		if err := compactAndOrderPartitionKeyEntries(ctx, tx, partitionKey, partitionRows); err != nil {
			slog.Error("failed to compact partition", "partition", partitionKey, "error", err)
			tx.Rollback()
			return nil, err
		}

		if err := tx.Commit(); err != nil {
			slog.Error("failed to commit transaction after compaction", "partition", partitionKey, "error", err)
			tx.Rollback()
			return nil, err
		}

		slog.Info("Compacted and ordered all partition entries",
			"tp_table", partitionKey.tpTable,
			"tp_partition", partitionKey.tpPartition,
			"tp_index", partitionKey.tpIndex,
			"year", partitionKey.year,
			"month", partitionKey.month,
			"input_files", partitionKey.fileCount,
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
func compactAndOrderPartitionKeyEntries(ctx context.Context, tx *sql.Tx, partitionKey partitionKey, pr *partitionKeyRows) error {

	slog.Debug("partition statistics",
		"tp_table", partitionKey.tpTable,
		"tp_partition", partitionKey.tpPartition,
		"tp_index", partitionKey.tpIndex,
		"year", partitionKey.year,
		"month", partitionKey.month,
		"row_count", pr.rowCount,
		"file_count", pr.fileCount,
		"max_rowid", pr.maxRowId,
		"min_timestamp", pr.minTimestamp,
		"max_timestamp", pr.maxTimestamp,
	)

	intervalDuration := pr.maxTimestamp.Sub(pr.minTimestamp)
	chunks := 1

	// If row count is greater than maxCompactionRowsPerChunk, calculate appropriate chunk interval
	if pr.rowCount > maxCompactionRowsPerChunk {
		// Calculate time interval to get approximately maxCompactionRowsPerChunk rows per chunk
		// Use hour-based intervals for more granular control
		chunks = (pr.rowCount + maxCompactionRowsPerChunk - 1) / maxCompactionRowsPerChunk // Ceiling division
		intervalDuration = intervalDuration / time.Duration(chunks)

		// Ensure minimum interval is at least 1 hour
		if intervalDuration < time.Hour {
			intervalDuration = time.Hour
		}
	}

	slog.Debug("processing partition in chunks",
		"total_rows", pr.rowCount,
		"chunks", chunks,
		"interval_duration", intervalDuration.String())

	// Process data in time-based chunks
	currentStart := pr.minTimestamp
	i := 1
	for currentStart.Before(pr.maxTimestamp) {
		currentEnd := currentStart.Add(intervalDuration)
		if currentEnd.After(pr.maxTimestamp) {
			currentEnd = pr.maxTimestamp
		}

		// For the final chunk, make it inclusive to catch the last row
		isFinalChunk := currentEnd.Equal(pr.maxTimestamp)

		if rowsInserted, err := insertOrderedDataForPartitionTimeRange(ctx, tx, partitionKey, currentStart, currentEnd, isFinalChunk); err != nil {
			return fmt.Errorf("failed to insert ordered data for time range %s to %s: %w",
				currentStart.Format("2006-01-02 15:04:05"),
				currentEnd.Format("2006-01-02 15:04:05"), err)
		}

		slog.Debug(fmt.Sprintf("processed chunk %d/%d", i, chunks))

		i++

		// Ensure next chunk starts exactly where this one ended to prevent gaps
		currentStart = currentEnd
	}

	slog.Debug("completed all time chunks for partition, deleting unordered entries",
		"tp_table", partitionKey.tpTable,
		"tp_partition", partitionKey.tpPartition,
		"tp_index", partitionKey.tpIndex,
		"year", partitionKey.year,
		"month", partitionKey.month,
		"max_rowid", pr.maxRowId)

	// we have sorted  and reinserted all data for this partition key - now delete all unordered entries (i.e. where rowid <  maxRowId)
	deleteQuery := fmt.Sprintf(`delete from "%s" 
		where tp_partition = ?
		  and tp_index = ?
		  and year(tp_timestamp) = ?
		  and month(tp_timestamp) = ?
		  and rowid <= ?`,
		partitionKey.tpTable)

	_, err := tx.ExecContext(ctx, deleteQuery,
		partitionKey.tpPartition,
		partitionKey.tpIndex,
		partitionKey.year,
		partitionKey.month,
		pr.maxRowId)
	if err != nil {
		return fmt.Errorf("failed to delete unordered data for partition: %w", err)
	}

	// Validate total rows processed matches original count
	finalCountQuery := fmt.Sprintf(`select count(*) from "%s" 
		where tp_partition = ?
		  and tp_index = ?
		  and year(tp_timestamp) = ?
		  and month(tp_timestamp) = ?`,
		partitionKey.tpTable)

	var finalRowCount int
	if err := tx.QueryRowContext(ctx, finalCountQuery,
		partitionKey.tpPartition,
		partitionKey.tpIndex,
		partitionKey.year,
		partitionKey.month).Scan(&finalRowCount); err != nil {
		return fmt.Errorf("failed to get final row count: %w", err)
	}

	if finalRowCount != pr.rowCount {
		return fmt.Errorf("total row count mismatch: expected %d, got %d", pr.rowCount, finalRowCount)
	}

	return nil
}

// insertOrderedDataForPartitionTimeRange inserts ordered data for a specific time range
func insertOrderedDataForPartitionTimeRange(ctx context.Context, tx *sql.Tx, partitionKey partitionKey, startTime, endTime time.Time, isFinalChunk bool) (int64, error) {
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
		partitionKey.tpTable,
		partitionKey.tpTable,
		timeCondition)

	result, err := tx.ExecContext(ctx, insertQuery,
		partitionKey.tpPartition,
		partitionKey.tpIndex,
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
