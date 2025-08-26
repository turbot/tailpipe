package parquet

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/turbot/pipe-fittings/v2/constants"

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

func orderDataFiles(ctx context.Context, db *database.DuckDb, patterns []PartitionPattern) (int, error) {
	slog.Info("Ordering DuckLake data files, 1 day at a time")

	/* we order data files as follows:
	 - get list of partition keys matching patterns
	 - for each  key , order entries <potentially split into day chunks>:
		- get max row id of rows with that partition key
	   	- reinsert ordered data for partition key
		- dedupe: delete rows for partition key with rowid <= prev max row id

	*/

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

	// TODO #compact benchmark and re-add trasactions
	// Start a transaction for all partition processing
	//tx, err := db.BeginTx(ctx, nil)
	//if err != nil {
	//	return 0, fmt.Errorf("failed to begin transaction: %w", err)
	//}
	//defer func() {
	//	if err != nil {
	//		if rbErr := tx.Rollback(); rbErr != nil {
	//			slog.Error("failed to rollback transaction", "error", rbErr)
	//		}
	//	}
	//}()

	// Process each partition
	for _, partitionKey := range partitionKeys {
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

		if err := compactAndOrderPartitionKeyEntries(ctx, db, partitionKey); err != nil {
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
		// now delete all entries for this partition key for previous snapshots
		if err := expirePrevSnapshotsForPartitionKey(ctx, db, partitionKey, maxSnapshotID); err != nil {
			return 0, err
		}
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

func compactAndOrderPartitionKeyEntries(ctx context.Context, tx *database.DuckDb, partitionKey partitionKey) error {
	// determine how many rows there are in the partition key and limit to 500K per query
	// TODO #cursor get row count and max rowid for partition key
	var maxRowId, rowCount int

	// todo #cursor determine how may chunks we need to split the dat ainto to limit to max 500K per chunk, and determine the interval
	//  (if full partition key is a month - split it)
	// then if there is more than one chunk, loop round processing one interval at a time, constructing timestamp filter

	// Get the year and month as integers for date calculations
	//year, _ := strconv.Atoi(partitionKey.year)
	//month, _ := strconv.Atoi(partitionKey.month)

	// Get the number of days in this month
	//daysInMonth := time.Date(year, time.Month(month+1), 0, 0, 0, 0, 0, time.UTC).Day()

	// Process each day separately
	//for day := 1; day <= daysInMonth; day++ {
	// Calculate start and end of day
	//startDate := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	//endDate := startDate.Add(24 * time.Hour)

	// Insert ordered data for this dayshanpsti
	insertQuery := fmt.Sprintf(`insert into "%s" 
			select * from "%s" 
			where tp_partition = %s
			  and tp_index = %s
			  and year(tp_timestamp) = %s
			  and month(tp_timestamp) = %s
			order by tp_timestamp`,
		partitionKey.tpTable, partitionKey.tpTable,
		EscapeLiteral(partitionKey.tpPartition),
		EscapeLiteral(partitionKey.tpIndex),
		EscapeLiteral(partitionKey.year),
		EscapeLiteral(partitionKey.month))

	//slog.Debug("compacting and ordering partition entries", "month", month, "day", day)
	if _, err := tx.ExecContext(ctx, insertQuery); err != nil {
		return fmt.Errorf("failed to insert ordered data for day")
	}
	//slog.Debug("finished compacting and ordering partition entries", "month", month, "day", day)
	//}

	//for day := 1; day <= daysInMonth; day++ {
	//	// Calculate start and end of day
	//	startDate := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	//	endDate := startDate.Add(24 * time.Hour)
	//
	//	// Insert ordered data for this dayshanpsti
	//	insertQuery := fmt.Sprintf(`insert into "%s"
	//		select * from "%s"
	//		where tp_partition = %s
	//		  and tp_index = %s
	//		  and year(tp_timestamp) = %s
	//		  and month(tp_timestamp) = %s
	//		  and tp_timestamp >= %s
	//		  and tp_timestamp < %s
	//		order by tp_timestamp`,
	//		partitionKey.tpTable, partitionKey.tpTable,
	//		EscapeLiteral(partitionKey.tpPartition),
	//		EscapeLiteral(partitionKey.tpIndex),
	//		EscapeLiteral(partitionKey.year),
	//		EscapeLiteral(partitionKey.month),
	//		EscapeLiteral(startDate.Format("2006-01-02 15:04:05")),
	//		EscapeLiteral(endDate.Format("2006-01-02 15:04:05")))
	//
	//	slog.Debug("compacting and ordering partition entries", "month", month, "day", day)
	//	if _, err := tx.ExecContext(ctx, insertQuery); err != nil {
	//		return fmt.Errorf("failed to insert ordered data for day %d: %w", day, err)
	//	}
	//	slog.Debug("finished compacting and ordering partition entries", "month", month, "day", day)
	//}

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

// expirePrevSnapshotsForPartitionKey expires all snapshots for a specific partition key
// that have snapshot IDs less than the given snapshot ID using DuckDB's built-in
// ducklake_expire_snapshots function.
//
// The function reads the snapshot time for the given snapshot ID and expires all
// snapshots older than that time + 1 second. This ensures we only expire snapshots
// that are definitely older than the current compaction snapshot.
//
// Note: We format the timestamp without timezone information because
// ducklake_expire_snapshots has a bug where it cannot parse timezone-aware
// timestamp strings (e.g., '+01' suffix) when using the older_than parameter.
func expirePrevSnapshotsForPartitionKey(ctx context.Context, tx *database.DuckDb, partitionKey partitionKey, oldMaxSnapshotId int64) error {
	// Read the snapshot time for the given snapshot ID
	var snapshotTimeStr string
	snapshotQuery := `SELECT snapshot_time FROM __ducklake_metadata_tailpipe_ducklake.ducklake_snapshot WHERE snapshot_id = ?`
	if err := tx.QueryRowContext(ctx, snapshotQuery, oldMaxSnapshotId).Scan(&snapshotTimeStr); err != nil {
		return fmt.Errorf("failed to read snapshot time for ID %d: %w", oldMaxSnapshotId, err)
	}

	// Parse the snapshot time and add 1 second for the expire threshold
	// The snapshot_time is stored as VARCHAR with timezone info like "2025-08-25 15:03:29.662+01"
	// We need to parse it and add 1 second, then format without timezone
	snapshotTime, err := time.Parse("2006-01-02 15:04:05.999-07", snapshotTimeStr)
	if err != nil {
		// Try alternative format without milliseconds
		snapshotTime, err = time.Parse("2006-01-02 15:04:05-07", snapshotTimeStr)
		if err != nil {
			return fmt.Errorf("failed to parse snapshot time '%s': %w", snapshotTimeStr, err)
		}
	}

	// Add 1 second to ensure we expire the snapshot associated with oldMaxSnapshotId
	expireTime := snapshotTime.Add(1 * time.Second).Format("2006-01-02 15:04:05")

	// Use ducklake_expire_snapshots to expire all snapshots older than the calculated time
	// This is more efficient and handles metadata cleanup properly
	expireQuery := fmt.Sprintf(`CALL ducklake_expire_snapshots('%s', dry_run => false, older_than => '%s')`,
		constants.DuckLakeCatalog, expireTime)

	slog.Debug("expiring previous snapshots for partition using ducklake_expire_snapshots",
		"tp_table", partitionKey.tpTable,
		"tp_partition", partitionKey.tpPartition,
		"tp_index", partitionKey.tpIndex,
		"year", partitionKey.year,
		"month", partitionKey.month,
		"snapshot_id", oldMaxSnapshotId,
		"snapshot_time", snapshotTimeStr,
		"expire_before_time", expireTime)

	if _, err := tx.ExecContext(ctx, expireQuery); err != nil {
		return fmt.Errorf("failed to expire previous snapshots for partition: %w", err)
	}

	return nil
}
