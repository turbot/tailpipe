package parquet

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
)

func DeletePartition(ctx context.Context, partition *config.Partition, from, to time.Time, db *database.DuckDb) (rowCount int, err error) {
	// TODO #DL HACK
	//  if we are using s3 do not delete for now as this does not work at present (need explicit S3 support I think)
	if envDir := os.Getenv("TAILPIPE_DATA_DIR"); strings.HasPrefix(envDir, "s3") {
		slog.Warn("Skipping partition deletion for S3 data source",
			"partition", partition.TableName,
			"from", from,
			"to", to,
		)
		return 0, nil // return 0 rows affected, not an error
	}

	// First check if the table exists using DuckLake metadata
	tableExistsQuery := fmt.Sprintf(`select exists (select 1 from %s.ducklake_table where table_name = ?)`, constants.DuckLakeMetadataCatalog)
	var tableExists bool
	if err := db.QueryRowContext(ctx, tableExistsQuery, partition.TableName).Scan(&tableExists); err != nil {
		return 0, fmt.Errorf("failed to check if table exists: %w", err)
	}

	if !tableExists {
		// Table doesn't exist, return 0 rows affected (not an error)
		return 0, nil
	}

	// build a delete query for the partition
	// Note: table names cannot be parameterized, so we use string formatting for the table name
	query := fmt.Sprintf(`delete from "%s" where tp_partition = ? and tp_date >= ? and tp_date <= ?`, partition.TableName)
	// Execute the query with parameters for the partition and date range
	result, err := db.ExecContext(ctx, query, partition.ShortName, from, to)
	if err != nil {
		return 0, fmt.Errorf("failed to delete partition: %w", err)
	}

	// Get the number of rows affected by the delete operation
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected count: %w", err)
	}
	rowCount = int(rowsAffected)

	// Only perform cleanup if we actually deleted some rows
	if rowCount > 0 {
		if err = DucklakeCleanup(ctx, db); err != nil {
			return 0, err
		}
	}

	return rowCount, nil
}

type partitionFileCount struct {
	tpTable          string
	tpPartition      string
	tpIndex          string
	tpTimestampMonth string
	fileCount        int
}

func CompactDataFilesManual(ctx context.Context, db *database.DuckDb, patterns []PartitionPattern) (*CompactionStatus, error) {
	var status = NewCompactionStatus()

	// get alist of partition key compbinations which have more that 1 parquet file
	partitionKeys, err := getPartitionKeysRequiringCompaction(ctx, db, patterns)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition keys requiring compaction: %w", err)
	}

	// now compact the files for each partition key
	for _, partitionKey := range partitionKeys {
		if partitionKey.fileCount <= 1 {
			slog.Debug("Skipping compaction for partition key",
				"tp_table", partitionKey.tpTable,
				"tp_partition", partitionKey.tpPartition,
				"tp_index", partitionKey.tpIndex,
				"tp_timestamp_month", partitionKey.tpTimestampMonth,
				"file_count", partitionKey.fileCount,
			)
			// if the file count is 1 or less, we do not need to compact
			// no need to compact, just increment the uncompacted count
			status.Uncompacted += partitionKey.fileCount
			continue
		}

		// increment the source file count by the file count for this partition key
		status.Source += partitionKey.fileCount
		if err := compactAndOrderPartitionEntries(ctx, db, partitionKey); err != nil {
			slog.Error("Failed to compact and order partition entries",
				"tp_table", partitionKey.tpTable,
				"tp_partition", partitionKey.tpPartition,
				"tp_index", partitionKey.tpIndex,
				"tp_timestamp_month", partitionKey.tpTimestampMonth,
				"file_count", partitionKey.fileCount,
				"error", err,
			)

			return nil, err
		}

		slog.Info("Compacted and ordered partition entries",
			"tp_table", partitionKey.tpTable,
			"tp_partition", partitionKey.tpPartition,
			"tp_index", partitionKey.tpIndex,
			"tp_timestamp_month", partitionKey.tpTimestampMonth,
			"source file_count", partitionKey.fileCount,
		)
		// increment the destination file count by 1 for each partition key
		status.Dest++
	}
	return status, nil

}

func compactAndOrderPartitionEntries(ctx context.Context, db *database.DuckDb, partitionKey partitionFileCount) error {
	// Create ordered snapshot for this partition combination
	// Only process partitions that have multiple files (fileCount > 1)
	snapshotQuery := fmt.Sprintf(`call ducklake.create_snapshot(
			'%s', '%s',
			snapshot_query => $$
				SELECT * FROM "%s"
				WHERE tp_partition = '%s' 
				  AND tp_index = '%s'
				  AND month(tp_timestamp) = '%s'
				ORDER BY tp_timestamp
			$$
		)`, constants.DuckLakeCatalog, partitionKey.tpTable, partitionKey.tpTable, partitionKey.tpPartition, partitionKey.tpIndex, partitionKey.tpTimestampMonth)

	if _, err := db.ExecContext(ctx, snapshotQuery); err != nil {
		return fmt.Errorf("failed to compact and order partition entries for tp_table %s, tp_partition %s, tp_index %s, month %s: %w",
			partitionKey.tpTable, partitionKey.tpPartition, partitionKey.tpIndex, partitionKey.tpTimestampMonth, err)
	}
	return nil
}

// query the ducklake_data_file table to get all partition keys combinations which satisfy the provided patterns,
// along with the file count for each partition key combination
func getPartitionKeysRequiringCompaction(ctx context.Context, db *database.DuckDb, patterns []PartitionPattern) ([]partitionFileCount, error) {
	// This query joins the DuckLake metadata tables to get partition key combinations:
	// - ducklake_data_file: contains file metadata and links to tables
	// - ducklake_file_partition_value: contains partition values for each file
	// - ducklake_table: contains table names
	//
	// The partition key structure is:
	// - fpv1 (index 0): tp_partition (e.g., "2024-07")
	// - fpv2 (index 1): tp_index (e.g., "index1")
	// - fpv3 (index 2): tp_timestamp month (e.g., "7" for July)
	//
	// We group by these partition keys and count files per combination,
	// filtering for active files (end_snapshot is null)
	query := `select 
  t.table_name as tp_table,
  fpv1.partition_value as tp_partition,
  fpv2.partition_value as tp_index,
  fpv3.partition_value as tp_timestamp,
  count(*) as file_count
from __ducklake_metadata_tailpipe_ducklake.ducklake_data_file df
join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv1
  on df.data_file_id = fpv1.data_file_id and fpv1.partition_key_index = 0
join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv2
  on df.data_file_id = fpv2.data_file_id and fpv2.partition_key_index = 1
join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv3
  on df.data_file_id = fpv3.data_file_id and fpv3.partition_key_index = 2
join __ducklake_metadata_tailpipe_ducklake.ducklake_table t
  on df.table_id = t.table_id
where df.end_snapshot is null
group by 
  t.table_name,
  fpv1.partition_value,
  fpv2.partition_value,
  fpv3.partition_value
order by file_count desc;`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition keys requiring compaction: %w", err)
	}

	defer rows.Close()
	var partitionKeys []partitionFileCount
	for rows.Next() {
		var partitionKey partitionFileCount
		if err := rows.Scan(&partitionKey.tpTable, &partitionKey.tpPartition, &partitionKey.tpIndex, &partitionKey.tpTimestampMonth, &partitionKey.fileCount); err != nil {
			return nil, fmt.Errorf("failed to scan partition key row: %w", err)
		}
		// check whether this partition key matches any of the provided patterns
		if PartitionMatchesPatterns(partitionKey.tpTable, partitionKey.tpPartition, patterns) {
			partitionKeys = append(partitionKeys, partitionKey)
		}
	}

	return partitionKeys, nil
}

func CompactDataFiles(ctx context.Context, db *database.DuckDb) (*CompactionStatus, error) {
	var status = NewCompactionStatus()

	// get the starting file count
	startingFileCount, err := parquetFileCount(ctx, db)
	if err != nil {
		return nil, err
	}
	// update status
	status.Source = startingFileCount

	// expire previous snapshots
	if err := expirePrevSnapshots(ctx, db); err != nil {
		return nil, err
	}

	// merge the the parquet files in the duckdb database
	if err := mergeParquetFiles(ctx, db); err != nil {
		return nil, err
	}

	// delete unused files
	if err := cleanupExpiredFiles(ctx, db); err != nil {
		return nil, err
	}

	// get the file count after merging and cleanup
	finalFileCount, err := parquetFileCount(ctx, db)
	if err != nil {
		return nil, err
	}
	// update status
	status.Dest = finalFileCount
	return status, nil
}

// DucklakeCleanup performs removes old snapshots deletes expired and unused parquet files from the DuckDB database.
func DucklakeCleanup(ctx context.Context, db *database.DuckDb) error {
	// now clean old snapshots
	if err := expirePrevSnapshots(ctx, db); err != nil {
		return err
	}
	// delete expired files
	if err := cleanupExpiredFiles(ctx, db); err != nil {
		return err
	}
	return nil
}

// mergeParquetFiles combines adjacent parquet files in the DuckDB database.
// thisa is how we achieve compaction
func mergeParquetFiles(ctx context.Context, db *database.DuckDb) error {
	if _, err := db.ExecContext(ctx, "call merge_adjacent_files();"); err != nil {
		if ctx.Err() != nil {
			return err
		}
		return fmt.Errorf("failed to merge parquet files: %w", err)
	}
	return nil
}

// expirePrevSnapshots expires all snapshots but the latest
// Ducklake stores a snapshot corresponding to each database operation - this allows the tracking of the history of changes
// However we do not need (currently) take advantage of this ducklake functionality, so we can remove all but the latest snapshot
// To do this we get the date of the most recent snapshot and then expire all snapshots older than that date.
// We then call ducklake_cleanup to remove the expired files.
func expirePrevSnapshots(ctx context.Context, db *database.DuckDb) error {
	// 1) get the timestamp of the latest snapshot from the metadata schema
	var latestTimestamp string
	query := fmt.Sprintf(`select snapshot_time from %s.ducklake_snapshot order by snapshot_id desc limit 1`, constants.DuckLakeMetadataCatalog)

	err := db.QueryRowContext(ctx, query).Scan(&latestTimestamp)
	if err != nil {
		return fmt.Errorf("failed to get latest snapshot timestamp: %w", err)
	}

	// 2) expire all snapshots older than the latest one
	// Note: ducklake_expire_snapshots uses named parameters which cannot be parameterized with standard SQL placeholders
	expireQuery := fmt.Sprintf(`call ducklake_expire_snapshots('%s', older_than => '%s')`, constants.DuckLakeCatalog, latestTimestamp)

	_, err = db.ExecContext(ctx, expireQuery)
	if err != nil {
		return fmt.Errorf("failed to expire old snapshots: %w", err)
	}

	return nil
}

// cleanupExpiredFiles deletes and files marked as expired in the ducklake system.
func cleanupExpiredFiles(ctx context.Context, db *database.DuckDb) error {
	cleanupQuery := fmt.Sprintf("call ducklake_cleanup_old_files('%s', cleanup_all => true)", constants.DuckLakeCatalog)

	_, err := db.ExecContext(ctx, cleanupQuery)
	if err != nil {
		return fmt.Errorf("failed to cleanup expired files: %w", err)
	}

	return nil
}

// parquetFileCount returns the count of ALL parquet files in the ducklake_data_file table (whether active or not)
func parquetFileCount(ctx context.Context, db *database.DuckDb) (int, error) {
	query := fmt.Sprintf(`select count (*) from %s.ducklake_data_file;`, constants.DuckLakeMetadataCatalog)

	var count int
	err := db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		if ctx.Err() != nil {
			return 0, err
		}
		return 0, fmt.Errorf("failed to get parquet file count: %w", err)
	}
	return count, nil
}
