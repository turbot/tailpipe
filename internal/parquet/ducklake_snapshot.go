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
func orderDataFiles(ctx context.Context, db *database.DuckDb, patterns []PartitionPattern) (int, error) {
	slog.Info("Ordering DuckLake data files")

	// get a list of partition key combinations which match any of the patterns
	partitionKeys, err := getPartitionKeysMatchingPattern(ctx, db, patterns)
	if err != nil {
		return 0, fmt.Errorf("failed to get partition keys requiring compaction: %w", err)
	}

	var uncompacted = 0
	if len(partitionKeys) == 0 {
		slog.Info("No matching partitions found for compaction")
		return 0, nil
	}

	// Process each partition
	for _, partitionKey := range partitionKeys {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			// This is a system failure - stop everything
			return 0, fmt.Errorf("failed to begin transaction for partition %v: %w", partitionKey, err)
		}

		// TODO #compact determine how fragmented this partition key is and only order if needed (unless 'force' is set?)
		// even a single parquet file might be unordered
		//if partitionKey.fileCount <= 1 {
		//	//
		//	uncompacted += partitionKey.fileCount
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

		if err := compactAndOrderPartitionKeyEntries(ctx, tx, partitionKey); err != nil {
			slog.Error("failed to compact partition", "partition", partitionKey, "error", err)
			tx.Rollback()
			return 0, err
		}

		if err := tx.Commit(); err != nil {
			slog.Error("failed to commit transaction after compaction", "partition", partitionKey, "error", err)
			tx.Rollback()
			return 0, err
		}

		slog.Info("Compacted and ordered all partition entries",
			"tp_table", partitionKey.tpTable,
			"tp_partition", partitionKey.tpPartition,
			"tp_index", partitionKey.tpIndex,
			"year", partitionKey.year,
			"month", partitionKey.month,
			"input_files", partitionKey.fileCount,
		)

		// TODO #compact think about file count totals
		//uncompacted += partitionKey.fileCount - 1
	}

	// TODO #compact benchmark and re-add trasactions
	//// Commit the transaction
	//if err = tx.Commit(); err != nil {
	//	return 0, fmt.Errorf("failed to commit transaction: %w", err)
	//}

	slog.Info("Finished ordering DuckLake data file")
	return uncompacted, nil
}

//	we order data files as follows:
//
// - get the row count, time range and max row id for the partition key
// - determine a time interval which will give us row counts <= maxCompactionRowsPerChunk
// - loop over time intervals. For each interval
//   - reinsert ordered data for partition key
//   - dedupe: delete rows for partition key with rowid <= prev max row id
func compactAndOrderPartitionKeyEntries(ctx context.Context, tx *sql.Tx, partitionKey partitionKey) error {
	// Get row count and time range for the partition key
	var rowCount, maxRowId int
	var minTimestamp, maxTimestamp time.Time

	// Query to get row count and time range for this partition
	countQuery := fmt.Sprintf(`select count(*), max(rowid) , min(tp_timestamp), max(tp_timestamp) from "%s" 
		where tp_partition = ?
		  and tp_index = ?
		  and year(tp_timestamp) = ?
		  and month(tp_timestamp) = ?`,
		partitionKey.tpTable)

	if err := tx.QueryRowContext(ctx, countQuery,
		partitionKey.tpPartition,
		partitionKey.tpIndex,
		partitionKey.year,
		partitionKey.month).Scan(&rowCount, &maxRowId, &minTimestamp, &maxTimestamp); err != nil {
		return fmt.Errorf("failed to get row count and time range for partition: %w", err)
	}

	slog.Debug("partition statistics",
		"tp_table", partitionKey.tpTable,
		"tp_partition", partitionKey.tpPartition,
		"tp_index", partitionKey.tpIndex,
		"year", partitionKey.year,
		"month", partitionKey.month,
		"row_count", rowCount,
		"min_timestamp", minTimestamp,
		"max_timestamp", maxTimestamp)

	intervalDuration := maxTimestamp.Sub(minTimestamp)
	chunks := 1

	// If row count is greater than maxCompactionRowsPerChunk, calculate appropriate chunk interval
	if rowCount > maxCompactionRowsPerChunk {
		// Calculate time interval to get approximately maxCompactionRowsPerChunk rows per chunk
		// Use hour-based intervals for more granular control
		chunks = (rowCount + maxCompactionRowsPerChunk - 1) / maxCompactionRowsPerChunk // Ceiling division
		intervalDuration = intervalDuration / time.Duration(chunks)

		// Ensure minimum interval is at least 1 hour
		if intervalDuration < time.Hour {
			intervalDuration = time.Hour
		}
	}

	slog.Debug("processing partition in chunks",
		"total_rows", rowCount,
		"chunks", chunks,
		"interval_duration", intervalDuration.String())

	// Process data in time-based chunks
	currentStart := minTimestamp
	i := 1
	for currentStart.Before(maxTimestamp) {
		currentEnd := currentStart.Add(intervalDuration)
		if currentEnd.After(maxTimestamp) {
			currentEnd = maxTimestamp
		}

		// For the final chunk, make it inclusive to catch the last row
		isFinalChunk := currentEnd.Equal(maxTimestamp)

		if err := insertOrderedDataForPartition(ctx, tx, partitionKey, currentStart, currentEnd, isFinalChunk); err != nil {
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
		"max_rowid", maxRowId)

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
		maxRowId)
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

	if finalRowCount != rowCount {
		return fmt.Errorf("total row count mismatch: expected %d, got %d", rowCount, finalRowCount)
	}

	return nil
}

// insertOrderedDataForPartition inserts ordered data for a specific time range
func insertOrderedDataForPartition(ctx context.Context, tx *sql.Tx, partitionKey partitionKey, startTime, endTime time.Time, isFinalChunk bool) error {
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

	if _, err := tx.ExecContext(ctx, insertQuery,
		partitionKey.tpPartition,
		partitionKey.tpIndex,
		startTime,
		endTime); err != nil {
		return fmt.Errorf("failed to insert ordered data for time range: %w", err)
	}

	return nil
}

// query the ducklake_data_file table to get all partition keys combinations which satisfy the provided patterns,
// along with the file count for each partition key combination
func getPartitionKeysMatchingPattern(ctx context.Context, db *database.DuckDb, patterns []PartitionPattern) ([]partitionKey, error) {
	// This query joins the DuckLake metadata tables to get partition key combinations:
	// - ducklake_data_file: contains file metadata and links to tables
	// - ducklake_file_partition_value: contains partition values for each file
	// - ducklake_table: contains table names
	//
	// The partition key structure is:
	// - fpv1 (index 0): tp_partition (e.g., "2024-07")
	// - fpv2 (index 1): tp_index (e.g., "index1")
	// - fpv3 (index 2): year(tp_timestamp) (e.g., "2024")
	// - fpv4 (index 3): month(tp_timestamp) (e.g., "7")
	//
	// We group by these partition keys and count files per combination,
	// filtering for active files (end_snapshot is null)
	// NOTE: Assumes partitions are defined in order: tp_partition (0), tp_index (1), year(tp_timestamp) (2), month(tp_timestamp) (3)
	query := `select 
  t.table_name as tp_table,
  fpv1.partition_value as tp_partition,
  fpv2.partition_value as tp_index,
  fpv3.partition_value as year,
  fpv4.partition_value as month,
  count(*) as file_count
from __ducklake_metadata_tailpipe_ducklake.ducklake_data_file df
join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv1
  on df.data_file_id = fpv1.data_file_id and fpv1.partition_key_index = 0
join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv2
  on df.data_file_id = fpv2.data_file_id and fpv2.partition_key_index = 1
join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv3
  on df.data_file_id = fpv3.data_file_id and fpv3.partition_key_index = 2
join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv4
  on df.data_file_id = fpv4.data_file_id and fpv4.partition_key_index = 3
join __ducklake_metadata_tailpipe_ducklake.ducklake_table t
  on df.table_id = t.table_id
where df.end_snapshot is null
group by 
  t.table_name,
  fpv1.partition_value,
  fpv2.partition_value,
  fpv3.partition_value,
  fpv4.partition_value
order by file_count desc;`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition keys requiring compaction: %w", err)
	}
	defer rows.Close()

	var partitionKeys []partitionKey
	for rows.Next() {
		var partitionKey partitionKey
		if err := rows.Scan(&partitionKey.tpTable, &partitionKey.tpPartition, &partitionKey.tpIndex, &partitionKey.year, &partitionKey.month, &partitionKey.fileCount); err != nil {
			return nil, fmt.Errorf("failed to scan partition key row: %w", err)
		}
		// check whether this partition key matches any of the provided patterns
		if PartitionMatchesPatterns(partitionKey.tpTable, partitionKey.tpPartition, patterns) {
			partitionKeys = append(partitionKeys, partitionKey)
		}
	}

	return partitionKeys, nil
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
