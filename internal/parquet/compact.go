package parquet

import (
	"context"
	"fmt"
	"golang.org/x/sync/semaphore"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/viper"
	"github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
)

func CompactDataFiles(ctx context.Context, updateFunc func(CompactionStatus), patterns ...PartitionPattern) error {
	// get the root data directory
	baseDir := config.GlobalWorkspaceProfile.GetDataDir()

	// open a duckdb connection
	db, err := database.NewDuckDb()
	if err != nil {
		return fmt.Errorf("failed to open duckdb connection: %w", err)
	}
	defer db.Close()

	// if the flag was provided, migrate the tp_index files
	if viper.GetBool(constants.ArgMigrateIndex) {
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

// if this parquetFile ends with the partition segment, return the table and partition
func getPartitionFromPath(dirPath string) (string, string, bool) {
	// if this is a partition folder, check if it matches the patterns
	parts := strings.Split(dirPath, "/")
	l := len(parts)
	if l > 1 && strings.HasPrefix(parts[l-1], "tp_partition=") && strings.HasPrefix(parts[l-2], "tp_table=") {

		table := strings.TrimPrefix(parts[l-2], "tp_table=")
		partition := strings.TrimPrefix(parts[l-1], "tp_partition=")
		return table, partition, true
	}
	return "", "", false
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

// addExtensionToFiles renames all given files to add a the provided extension
func addExtensionToFiles(fileNames []string, suffix string) ([]string, error) {
	var renamedFiles []string
	for _, file := range fileNames {
		newFile := file + suffix
		renamedFiles = append(renamedFiles, newFile)
		if err := os.Rename(file, newFile); err != nil {
			// try to rename all files we have already renamed back to their original names
			for _, renamedFile := range renamedFiles {
				// remove the .compacted extension
				originalFile := strings.TrimSuffix(renamedFile, ".compacted")
				if err := os.Rename(renamedFile, originalFile); err != nil {
					slog.Warn("Failed to rename parquet file back to original name", "file", renamedFile, "error", err)
				}
			}
			return nil, fmt.Errorf("failed to rename parquet file %s to %s: %w", file, newFile, err)
		}
	}
	return renamedFiles, nil
}

// removeExtensionFromFiles renames all given files to remove the provided extension
func removeExtensionFromFiles(fileNames []string, suffix string) error {
	var renamedFiles []string
	for _, file := range fileNames {
		if !strings.HasSuffix(file, suffix) {
			continue // skip files that do not have the suffix
		}
		newFile := strings.TrimSuffix(file, suffix)
		renamedFiles = append(renamedFiles, newFile)
		if err := os.Rename(file, newFile); err != nil {
			// try to rename all files we have already renamed back to their original names
			for _, renamedFile := range renamedFiles {
				if err := os.Rename(renamedFile, renamedFile+suffix); err != nil {
					slog.Warn("Failed to rename parquet file back to original name", "file", renamedFile, "error", err)
				}
			}
			return fmt.Errorf("failed to rename parquet file %s to %s: %w", file, newFile, err)
		}
	}
	return nil
}

// deleteFilesConcurrently deletes the given parquet files concurrently, ensuring that empty parent directories are
// also cleaned recursively up to the baseDir.
func deleteFilesConcurrently(ctx context.Context, parquetFiles []string, baseDir string) error {
	const maxConcurrentDeletions = 5
	sem := semaphore.NewWeighted(int64(maxConcurrentDeletions))
	var wg sync.WaitGroup
	var failures int32

	dirSet := make(map[string]struct{})
	var dirMu sync.Mutex

	for _, file := range parquetFiles {
		wg.Add(1)
		go func(file string) {
			defer wg.Done()

			if err := sem.Acquire(ctx, 1); err != nil {
				atomic.AddInt32(&failures, 1)
				return
			}
			defer sem.Release(1)

			if err := os.Remove(file); err != nil {
				atomic.AddInt32(&failures, 1)
				return
			}

			// Collect parent directory for cleanup
			parentDir := filepath.Dir(file)
			dirMu.Lock()
			dirSet[parentDir] = struct{}{}
			dirMu.Unlock()
		}(file)
	}

	wg.Wait()

	// Recursively delete empty parent dirs up to baseDir
	for startDir := range dirSet {
		deleteEmptyParentsUpTo(startDir, baseDir)
	}

	if atomic.LoadInt32(&failures) > 0 {
		return fmt.Errorf("failed to delete %d parquet %s", failures, utils.Pluralize("file", int(failures)))
	}
	return nil
}

// deleteEmptyParentsUpTo deletes empty directories upward from startDir up to (but not including) baseDir.
func deleteEmptyParentsUpTo(startDir, baseDir string) {
	baseDirAbs, err := filepath.Abs(baseDir)
	if err != nil {
		return // fail-safe: don't recurse without a valid base
	}
	current := startDir

	for {
		currentAbs, err := filepath.Abs(current)
		if err != nil {
			return
		}

		// Stop if we've reached or passed the baseDir
		if currentAbs == baseDirAbs || !strings.HasPrefix(currentAbs, baseDirAbs) {
			return
		}

		entries, err := os.ReadDir(current)
		if err != nil || len(entries) > 0 {
			return // non-empty or inaccessible
		}

		_ = os.Remove(current) // delete and continue upward
		current = filepath.Dir(current)
	}
}
