package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/briandowns/spinner"
	"github.com/spf13/cobra"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/cmdconfig"
	"github.com/turbot/pipe-fittings/contexthelpers"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe/internal/config"
	"golang.org/x/sync/semaphore"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func compactCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compact [flags]",
		Args:  cobra.ArbitraryArgs,
		Run:   runCompactCmd,
		Short: "compact the data files",
		Long:  `compact the parquet data files into one file per day.`,
	}

	cmdconfig.OnCmd(cmd)
	return cmd
}

func runCompactCmd(cmd *cobra.Command, _ []string) {
	var err error
	ctx, cancel := context.WithCancel(cmd.Context())
	contexthelpers.StartCancelHandler(cancel)

	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
		if err != nil {
			setExitCodeForCompactError(err)
			error_helpers.ShowError(ctx, err)
		}
	}()

	s := spinner.New(
		spinner.CharSets[14],
		100*time.Millisecond,
		spinner.WithHiddenCursor(true),
		spinner.WithWriter(os.Stdout),
	)

	// start and stop spinner around the processing
	s.Start()
	defer s.Stop()
	s.Suffix = " compacting parquet files"

	// define func to update the spinner suffix with the number of files compacted
	var sourceCount, destCount int
	updateTotals := func(sourceInc, destInc int) {
		sourceCount += sourceInc
		destCount += destInc
		s.Suffix = fmt.Sprintf(" compacting parquet files (%d files -> %d files)", sourceCount, destCount)
	}

	err = compactDataFiles(ctx, updateTotals)
	if err == nil {
		fmt.Printf("\nCompacted %d files into %d files\n\n", sourceCount, destCount)
	} else if ctx.Err() != nil {
		fmt.Printf("\nCompaction cancelled: Compacted %d files into %d files\n\n", sourceCount, destCount)
		// clear error so we do not display it
		err = nil
	}
}

func compactDataFiles(ctx context.Context, updateFunc func(filesCompacted int, compactedCount int)) error {
	// get the root data directory
	baseDir := config.GlobalWorkspaceProfile.GetDataDir()

	// open a duckdb connection
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("failed to open duckdb connection: %w", err)
	}
	defer db.Close()

	// traverse the directory and compact files
	err = traverseAndCompact(ctx, db, baseDir, updateFunc)
	return err

}

func traverseAndCompact(ctx context.Context, db *sql.DB, dirPath string, updateFunc func(int, int)) error {
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
			err := traverseAndCompact(ctx, db, subDirPath, updateFunc)
			if err != nil {
				return err
			}
		} else if strings.HasSuffix(entry.Name(), ".parquet") {
			// collect parquet file paths
			parquetFiles = append(parquetFiles, filepath.Join(dirPath, entry.Name()))
		}
	}
	if len(parquetFiles) < 2 {
		return nil
	}

	log.Printf("processing directory: %s", dirPath)
	err = compactParquetFiles(ctx, db, parquetFiles, dirPath)
	if err != nil {
		if ctx.Err() != nil {
			return err
		}
		return fmt.Errorf("failed to compact parquet files in %s: %w", dirPath, err)
	}

	// update the totals
	updateFunc(len(parquetFiles), 1)

	return nil
}

func compactParquetFiles(ctx context.Context, db *sql.DB, parquetFiles []string, inputPath string) (err error) {
	now := time.Now()
	compactedFileName := fmt.Sprintf("snap_%s_%06d.parquet", now.Format("20060102150405"), now.Nanosecond()/1000)

	// define temp and output file paths
	tempOutputFile := filepath.Join(inputPath, compactedFileName+".tmp")
	outputFile := filepath.Join(inputPath, compactedFileName)

	defer func() {
		if err != nil {
			slog.Error("Compaction failed", "inputPath", inputPath, "error", err)
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

func setExitCodeForCompactError(err error) {
	// set exit code only if an error occurred and no exit code is already set
	if exitCode != 0 || err == nil {
		return
	}
	exitCode = 1
}
