package parquet

import (
	"context"
	"database/sql"
	"fmt"
	"golang.org/x/sync/semaphore"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe/internal/config"
)

type CompactionStatus struct {
	Source      int
	Dest        int
	Uncompacted int
}

func (total *CompactionStatus) Update(counts CompactionStatus) {
	total.Source += counts.Source
	total.Dest += counts.Dest
	total.Uncompacted += counts.Uncompacted
}

func (total *CompactionStatus) VerboseString() string {
	if total.Source == 0 && total.Dest == 0 && total.Uncompacted == 0 {
		return "No files to compact."
	}

	uncompactedString := ""
	if total.Uncompacted > 0 {
		uncompactedString = fmt.Sprintf("%d files did not need compaction.", total.Uncompacted)
	}

	// if nothing was compacated, just print the uncompacted string
	if total.Source == 0 {
		return fmt.Sprintf("%s\n\n", uncompactedString)
	}

	// add brackets around the uncompacted string (if needed)
	if len(uncompactedString) > 0 {
		uncompactedString = fmt.Sprintf(" (%s)", uncompactedString)
	}

	return fmt.Sprintf("Compacted %d files into %d files.%s\n", total.Source, total.Dest, uncompactedString)
}

func (total *CompactionStatus) BriefString() string {
	if total.Source == 0 {
		return ""
	}

	uncompactedString := ""
	if total.Uncompacted > 0 {
		uncompactedString = fmt.Sprintf(" (%d files did not need compaction.)", total.Uncompacted)
	}

	return fmt.Sprintf("Compacted %d files into %d files.%s\n", total.Source, total.Dest, uncompactedString)
}

func CompactDataFiles(ctx context.Context, updateFunc func(CompactionStatus), patterns ...PartitionPattern) error {
	// get the root data directory
	baseDir := config.GlobalWorkspaceProfile.GetDataDir()

	// open a duckdb connection
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("failed to open duckdb connection: %w", err)
	}
	defer db.Close()

	// traverse the directory and compact files
	return traverseAndCompact(ctx, db, baseDir, updateFunc, patterns)
}
func traverseAndCompact(ctx context.Context, db *sql.DB, dirPath string, updateFunc func(CompactionStatus), patterns []PartitionPattern) error {
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

// if this filepath ends with the partition segment, return the table and partition
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

func compactParquetFiles(ctx context.Context, db *sql.DB, parquetFiles []string, inputPath string) (err error) {
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
	//nolint:gosec // read_parquet and copy do not support params - and this is a trusted source
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
	err = renameCompactedFiles(parquetFiles)
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
	err = deleteCompactedFiles(ctx, parquetFiles)

	// TODO dedupe rows ignoring ingestion timestamp and tp_index? https://github.com/turbot/tailpipe/issues/69

	return nil
}

// renameCompactedFiles renames all parquet files to add a .compacted extension
func renameCompactedFiles(parquetFiles []string) error {

	var renamedFiles []string
	for _, file := range parquetFiles {
		newFile := file + ".compacted"
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
			return fmt.Errorf("failed to rename parquet file %s to %s: %w", file, newFile, err)
		}
	}
	return nil
}

// deleteCompactedFiles deletes all parquet files with a .compacted extension
func deleteCompactedFiles(ctx context.Context, parquetFiles []string) error {
	const maxConcurrentDeletions = 5
	sem := semaphore.NewWeighted(int64(maxConcurrentDeletions))
	var wg sync.WaitGroup
	var failures int32

	for _, file := range parquetFiles {
		wg.Add(1)
		go func(file string) {
			defer wg.Done()

			// Acquire a slot in the semaphore
			if err := sem.Acquire(ctx, 1); err != nil {
				atomic.AddInt32(&failures, 1)
				return
			}
			defer sem.Release(1)

			newFile := file + ".compacted"
			if err := os.Remove(newFile); err != nil {
				atomic.AddInt32(&failures, 1)
			}
		}(file)
	}

	// Wait for all deletions to complete
	wg.Wait()

	// Check failure count atomically
	failureCount := atomic.LoadInt32(&failures)
	if failureCount > 0 {
		return fmt.Errorf("failed to delete %d parquet %s", failureCount, utils.Pluralize("file", int(failureCount)))
	}
	return nil
}
