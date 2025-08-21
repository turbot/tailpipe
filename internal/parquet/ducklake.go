package parquet

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/statushooks"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
)

func DeletePartition(ctx context.Context, partition *config.Partition, from, to time.Time, db *database.DuckDb) (rowCount int, err error) {
	spinner := statushooks.NewStatusSpinnerHook()
	spinner.Show()
	defer spinner.Hide()
	spinner.SetStatus(fmt.Sprintf("Deleting partition %s", partition.TableName))
	// TODO #DL https://github.com/turbot/tailpipe/issues/505
	//  if we are using s3 do not delete for now as this does not work at present (need explicit S3 support I think)
	//  remove before release https://github.com/turbot/tailpipe/issues/520
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
	query := fmt.Sprintf(`delete from "%s" where tp_partition = ? and tp_timestamp >= ? and tp_timestamp <= ?`, partition.TableName)
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

// DucklakeCleanup performs removes old snapshots deletes expired and unused parquet files from the DuckDB database.
func DucklakeCleanup(ctx context.Context, db *database.DuckDb) error {
	slog.Info("Cleaning up DuckLake snapshots and expired files")
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

// expirePrevSnapshots expires all snapshots but the latest
// Ducklake stores a snapshot corresponding to each database operation - this allows the tracking of the history of changes
// However we do not need (currently) take advantage of this ducklake functionality, so we can remove all but the latest snapshot
// To do this we get the date of the most recent snapshot and then expire all snapshots older than that date.
// We then call ducklake_cleanup to remove the expired files.
func expirePrevSnapshots(ctx context.Context, db *database.DuckDb) error {
	slog.Info("Expiring old DuckLake snapshots")
	defer slog.Info("DuckLake snapshot expiration complete")

	// 1) get the timestamp of the latest snapshot from the metadata schema
	var latestTimestamp string
	query := fmt.Sprintf(`select snapshot_time from %s.ducklake_snapshot order by snapshot_id desc limit 1`, constants.DuckLakeMetadataCatalog)

	err := db.QueryRowContext(ctx, query).Scan(&latestTimestamp)
	if err != nil {
		return fmt.Errorf("failed to get latest snapshot timestamp: %w", err)
	}

	// Parse the snapshot time
	// NOTE: rather than cast as timestamp, we read as a string then remove any timezone component
	// This is because of the dubious behaviour of ducklake_expire_snapshots described below
	parsedTime, err := time.Parse("2006-01-02 15:04:05.999-07", latestTimestamp)
	if err != nil {
		if err != nil {
			return fmt.Errorf("failed to parse snapshot time '%s': %w", latestTimestamp, err)
		}
	}
	// format the time
	// TODO Note: ducklake_expire_snapshots expects a local time without timezone,
	//  i.e if the time is '2025-08-26 13:25:10.365 +0100', we should pass '2025-08-26 13:25:10.365'
	//  We need to raise a ducklake issue
	formattedTime := parsedTime.Format("2006-01-02 15:04:05.000")

	slog.Debug("Latest snapshot timestamp", "timestamp", latestTimestamp)

	// 2) expire all snapshots older than the latest one
	// Note: ducklake_expire_snapshots uses named parameters which cannot be parameterized with standard SQL placeholders
	expireQuery := fmt.Sprintf(`call ducklake_expire_snapshots('%s', older_than => '%s')`, constants.DuckLakeCatalog, formattedTime)

	_, err = db.ExecContext(ctx, expireQuery)
	if err != nil {
		return fmt.Errorf("failed to expire old snapshots: %w", err)
	}

	return nil
}

// cleanupExpiredFiles deletes and files marked as expired in the ducklake system.
func cleanupExpiredFiles(ctx context.Context, db *database.DuckDb) error {
	slog.Info("Cleaning up expired files in DuckLake")
	defer slog.Info("DuckLake expired files cleanup complete")

	cleanupQuery := fmt.Sprintf("call ducklake_cleanup_old_files('%s', cleanup_all => true)", constants.DuckLakeCatalog)

	_, err := db.ExecContext(ctx, cleanupQuery)
	if err != nil {
		return fmt.Errorf("failed to cleanup expired files: %w", err)
	}

	return nil
}
