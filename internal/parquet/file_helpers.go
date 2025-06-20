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

	"github.com/turbot/pipe-fittings/v2/utils"
)

// if this parquetFile ends with the partition segment, return the table and partition
func getPartitionFromPath(dirPath string) (string, string, bool) {
	// if this is a partition folder, check if it matches the patterns
	parts := strings.Split(dirPath, "/")
	l := len(parts)
	if l < 2 {
		return "", "", false
	}

	// Find the last two segments that match our pattern
	for i := l - 1; i > 0; i-- {
		if strings.HasPrefix(parts[i], "tp_partition=") && strings.HasPrefix(parts[i-1], "tp_table=") {
			table := strings.TrimPrefix(parts[i-1], "tp_table=")
			partition := strings.TrimPrefix(parts[i], "tp_partition=")
			return table, partition, true
		}
	}
	return "", "", false
}

// addExtensionToFiles renames all given files to add a the provided extension
func addExtensionToFiles(fileNames []string, suffix string) ([]string, error) {
	if len(fileNames) == 0 {
		return []string{}, nil
	}
	var renamedFiles []string
	for _, file := range fileNames {
		// Check if file exists
		if _, err := os.Stat(file); os.IsNotExist(err) {
			return nil, fmt.Errorf("file does not exist: %s", file)
		}

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
