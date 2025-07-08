package parquet

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/viper"
	"github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/tailpipe/internal/config"
	localconstants "github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
)

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

func mergeParquetFiles(ctx context.Context, db *database.DuckDb) error {
	//CALL catalog.merge_adjacent_files();
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
func CompactDataFilesLegacy(ctx context.Context, updateFunc func(CompactionStatus), patterns ...PartitionPattern) error {
	// get the root data directory
	baseDir := config.GlobalWorkspaceProfile.GetDataDir()

	// open a duckdb connection
	db, err := database.NewDuckDb()
	if err != nil {
		return fmt.Errorf("failed to open duckdb connection: %w", err)
	}
	defer db.Close()

	// if the flag was provided, migrate the tp_index files
	if viper.GetBool(constants.ArgReindex) {
		// traverse the directory and migrate files
		if err := migrateTpIndex(ctx, db, baseDir, updateFunc, patterns); err != nil {
			return err
		}
	}

	// traverse the directory and compact files
	if err := traverseAndCompact(ctx, db, baseDir, updateFunc, patterns); err != nil {
		return err
	}

	// now delete any invalid parquet files that match the patterns
	invalidDeleteErr := deleteInvalidParquetFiles(config.GlobalWorkspaceProfile.GetDataDir(), patterns)
	if invalidDeleteErr != nil {
		slog.Warn("Failed to delete invalid parquet files", "error", invalidDeleteErr)
	}
	return nil
}

func traverseAndCompact(ctx context.Context, db *database.DuckDb, dirPath string, updateFunc func(CompactionStatus), patterns []PartitionPattern) error {
	// if this is the partition folder, check if it matches the patterns before descending further
	if table, partition, ok := getPartitionFromPath(dirPath); ok {
		if !PartitionMatchesPatterns(table, partition, patterns) {
			return nil
		}
	}

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("failed to read directory %s: %w", dirPath, err)
	}

	var parquetFiles []string

	// process directory entries
	for _, entry := range entries {
		if entry.IsDir() {
			// recursively process subdirectories
			subDirPath := filepath.Join(dirPath, entry.Name())
			err := traverseAndCompact(ctx, db, subDirPath, updateFunc, patterns)
			if err != nil {
				return err
			}
		} else if strings.HasSuffix(entry.Name(), ".parquet") {
			// collect parquet file paths
			parquetFiles = append(parquetFiles, filepath.Join(dirPath, entry.Name()))
		}
	}
	numFiles := len(parquetFiles)
	if numFiles < 2 {
		// nothing to compact - update the totals anyway so we include uncompacted files in the overall total
		updateFunc(CompactionStatus{Uncompacted: numFiles})
		return nil
	}

	err = compactParquetFiles(ctx, db, parquetFiles, dirPath)
	if err != nil {
		if ctx.Err() != nil {
			return err
		}
		return fmt.Errorf("failed to compact parquet files in %s: %w", dirPath, err)
	}

	// update the totals
	updateFunc(CompactionStatus{Source: numFiles, Dest: 1})

	return nil
}

// compactParquetFiles compacts the given parquet files into a single file in the specified inputPath.
func compactParquetFiles(ctx context.Context, db *database.DuckDb, parquetFiles []string, inputPath string) (err error) {
	now := time.Now()
	compactedFileName := fmt.Sprintf("snap_%s_%06d.parquet", now.Format("20060102150405"), now.Nanosecond()/1000)

	if !filepath.IsAbs(inputPath) {
		return fmt.Errorf("inputPath must be an absolute path")
	}
	// define temp and output file paths
	tempOutputFile := filepath.Join(inputPath, compactedFileName+".tmp")
	outputFile := filepath.Join(inputPath, compactedFileName)

	defer func() {
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("Compaction failed", "inputPath", inputPath, "error", err)
			}
			// delete temp file if it exists
			_ = os.Remove(tempOutputFile)
		}
	}()

	// compact files using duckdb
	query := fmt.Sprintf(`
		copy (
			select * from read_parquet('%s/*.parquet')
		) to '%s' (format parquet, overwrite true);
	`, inputPath, tempOutputFile)

	if _, err := db.ExecContext(ctx, query); err != nil {
		if ctx.Err() != nil {
			return err
		}
		return fmt.Errorf("failed to compact parquet files: %w", err)
	}

	// rename all parquet files to add a .compacted extension
	renamedSourceFiles, err := addExtensionToFiles(parquetFiles, ".compacted")
	if err != nil {
		// delete the temp file
		_ = os.Remove(tempOutputFile)
		return err
	}

	// rename temp file to final output file
	if err := os.Rename(tempOutputFile, outputFile); err != nil {
		return fmt.Errorf("failed to rename temp file %s to %s: %w", tempOutputFile, outputFile, err)
	}

	// finally, delete renamed source parquet files
	err = deleteFilesConcurrently(ctx, renamedSourceFiles, config.GlobalWorkspaceProfile.GetDataDir())

	return nil
}
