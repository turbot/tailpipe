package parquet

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/turbot/tailpipe/internal/database"
)

type partitionKey struct {
	tpTable     string
	tpPartition string
	tpIndex     string
	year        string // year(tp_timestamp) from partition value
	month       string // month(tp_timestamp) from partition value
	fileCount   int
}

func compactDataFilesManual(ctx context.Context, db *database.DuckDb, patterns []PartitionPattern) (int, error) {
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

	// get the current max snapshot id
	var maxSnapshotID int64
	maxSnapshotQuery := `select max(snapshot_id) from __ducklake_metadata_tailpipe_ducklake.ducklake_snapshot`
	if err = db.QueryRowContext(ctx, maxSnapshotQuery).Scan(&maxSnapshotID); err != nil {
		return 0, fmt.Errorf("failed to get max snapshot ID: %w", err)
	}
	slog.Debug("got max snapshot ID", "max_snapshot_id", maxSnapshotID)

	// Start a transaction for all partition processing
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				slog.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	// Process each partition
	for _, partitionKey := range partitionKeys {
		if partitionKey.fileCount <= 1 {
			uncompacted += partitionKey.fileCount
			continue
		}

		slog.Debug("Compacting partition entries",
			"tp_table", partitionKey.tpTable,
			"tp_partition", partitionKey.tpPartition,
			"tp_index", partitionKey.tpIndex,
			"year", partitionKey.year,
			"month", partitionKey.month,
			"file_count", partitionKey.fileCount,
		)

		if err := compactAndOrderPartitionEntries(ctx, tx, partitionKey); err != nil {
			return 0, err
		}

		slog.Info("Compacted and ordered partition entries",
			"tp_table", partitionKey.tpTable,
			"tp_partition", partitionKey.tpPartition,
			"tp_index", partitionKey.tpIndex,
			"year", partitionKey.year,
			"month", partitionKey.month,
			"input_files", partitionKey.fileCount,
			"output_files", 1,
		)
		// now delete all entries for this partition key for previou ssnapshots
		if err := deletePreveSnapshotsForPartitionKey(ctx, tx, partitionKey, maxSnapshotID); err != nil {
			return 0, err
		}
		uncompacted += partitionKey.fileCount - 1

	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return uncompacted, nil
}

func compactAndOrderPartitionEntries(ctx context.Context, tx *sql.Tx, partitionKey partitionKey) error {
	// Get the year and month as integers for date calculations
	year, _ := strconv.Atoi(partitionKey.year)
	month, _ := strconv.Atoi(partitionKey.month)

	// Get the number of days in this month
	daysInMonth := time.Date(year, time.Month(month+1), 0, 0, 0, 0, 0, time.UTC).Day()

	// Process each day separately
	for day := 1; day <= daysInMonth; day++ {
		// Calculate start and end of day
		startDate := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
		endDate := startDate.Add(24 * time.Hour)

		// Insert ordered data for this dayshanpsti
		insertQuery := fmt.Sprintf(`insert into "%s" 
			select * from "%s" 
			where tp_partition = %s
			  and tp_index = %s
			  and year(tp_timestamp) = %s
			  and month(tp_timestamp) = %s
			  and tp_timestamp >= %s
			  and tp_timestamp < %s
			order by tp_timestamp`,
			partitionKey.tpTable, partitionKey.tpTable,
			EscapeLiteral(partitionKey.tpPartition),
			EscapeLiteral(partitionKey.tpIndex),
			EscapeLiteral(partitionKey.year),
			EscapeLiteral(partitionKey.month),
			EscapeLiteral(startDate.Format("2006-01-02 15:04:05")),
			EscapeLiteral(endDate.Format("2006-01-02 15:04:05")))

		slog.Debug("compacting and ordering partition entries", "month", month, "day", day)
		if _, err := tx.ExecContext(ctx, insertQuery); err != nil {
			return fmt.Errorf("failed to insert ordered data for day %d: %w", day, err)
		}
		slog.Debug("finished compacting and ordering partition entries", "month", month, "day", day)
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

// deletePreveSnapshotsForPartitionKey deletes all entries for a specific partition key
// that have snapshot IDs less than or equal to the given snapshot ID
func deletePreveSnapshotsForPartitionKey(ctx context.Context, tx *sql.Tx, partitionKey partitionKey, oldMaxSnapshotId int64) error {
	deleteQuery := fmt.Sprintf(`delete from "%s" 
	where tp_partition = %s
	  and tp_index = %s
	  and year(tp_timestamp) = %s
	  and month(tp_timestamp) = %s
	  and snapshot_id <= %d`,
		partitionKey.tpTable,
		EscapeLiteral(partitionKey.tpPartition),
		EscapeLiteral(partitionKey.tpIndex),
		EscapeLiteral(partitionKey.year),
		EscapeLiteral(partitionKey.month),
		oldMaxSnapshotId)

	slog.Debug("deleting previous snapshots for partition",
		"tp_table", partitionKey.tpTable,
		"tp_partition", partitionKey.tpPartition,
		"tp_index", partitionKey.tpIndex,
		"year", partitionKey.year,
		"month", partitionKey.month,
		"delete_before_snapshot_id", oldMaxSnapshotId)

	if _, err := tx.ExecContext(ctx, deleteQuery); err != nil {
		return fmt.Errorf("failed to delete previous snapshots for partition: %w", err)
	}

	return nil
}
