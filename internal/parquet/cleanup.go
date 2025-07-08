package parquet

import (
	"context"
	"fmt"
	"time"

	"github.com/turbot/tailpipe/internal/config"
	localconstants "github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
)

func DeletePartition(ctx context.Context, partition *config.Partition, from, to time.Time) (rowCount int, err error) {
	db, err := database.NewDuckDb(database.WithDuckLakeEnabled(true))
	if err != nil {
		return 0, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer db.Close()

	// build a delete query for the partition
	// Note: table names cannot be parameterized, so we use string formatting for the table name
	query := fmt.Sprintf(`delete from %s.%s where tp_partition = ? and tp_date >= ? and tp_date <= ?`, localconstants.DuckLakeSchema, partition.TableName)
	// Execute the query with parameters for the partition and date range
	result, err := db.Exec(query, partition.ShortName, from, to)
	if err != nil {
		return 0, fmt.Errorf("failed to delete partition: %w", err)
	}

	// Get the number of rows affected by the delete operation
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected count: %w", err)
	}
	rowCount = int(rowsAffected)

	if err = DucklakeCleanup(ctx, db); err != nil {
		return 0, err
	}

	return rowCount, nil
}

func CompactDataFiles(ctx context.Context) (*CompactionStatus, error) {
	var status = NewCompactionStatus()

	// open a duckdb connection
	db, err := database.NewDuckDb(database.WithDuckLakeEnabled(true))
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb connection: %w", err)
	}
	defer db.Close()

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
func mergeParquetFiles(ctx context.Context, db *database.DuckDb) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("call %s.merge_adjacent_files();", localconstants.DuckLakeSchema)); err != nil {
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
	query := fmt.Sprintf(`select snapshot_time from %s.ducklake_snapshot order by snapshot_id desc limit 1`, localconstants.DuckLakeMetadataSchema)

	err := db.QueryRowContext(ctx, query).Scan(&latestTimestamp)
	if err != nil {
		return fmt.Errorf("failed to get latest snapshot timestamp: %w", err)
	}

	// 2) expire all snapshots older than the latest one
	expireQuery := fmt.Sprintf(`call ducklake_expire_snapshots('%s', older_than => '%s')`, localconstants.DuckLakeSchema, latestTimestamp)

	_, err = db.ExecContext(ctx, expireQuery)
	if err != nil {
		return fmt.Errorf("failed to expire old snapshots: %w", err)
	}

	return nil
}

// cleanupExpiredFiles deletes and files marked as expired in the ducklake system.
func cleanupExpiredFiles(ctx context.Context, db *database.DuckDb) error {
	cleanupQuery := fmt.Sprintf(`call ducklake_cleanup_old_files('%s', cleanup_all => true)`, localconstants.DuckLakeSchema)

	_, err := db.ExecContext(ctx, cleanupQuery)
	if err != nil {
		return fmt.Errorf("failed to cleanup expired files: %w", err)
	}

	return nil
}

// parquetFileCount returns the count of ALL parquet files in the ducklake_data_file table (whether active or not)
func parquetFileCount(ctx context.Context, db *database.DuckDb) (int, error) {

	query := fmt.Sprintf(`select count (*) from %s.ducklake_data_file;`, localconstants.DuckLakeMetadataSchema)

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
