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
	"github.com/turbot/tailpipe/internal/config"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const compactedFileName = "compacted.parquet"

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

	fileCount, compactedCount, err := compactDataFiles(ctx, s)

	if err == nil {
		fmt.Printf("\nCompacted %d files into %d files\n\n", fileCount, compactedCount)
	} else if ctx.Err() != nil {
		fmt.Printf("\nCompaction cancelled: Compacted %d files into %d files\n\n", fileCount, compactedCount)
		// clear error so we do not display it
		err = nil
	}
}
func compactDataFiles(ctx context.Context, s *spinner.Spinner) (int, int, error) {
	// get the root data directory
	baseDir := config.GlobalWorkspaceProfile.GetDataDir()

	// open a duckdb connection
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open duckdb connection: %w", err)
	}
	defer db.Close()

	// start and stop spinner around the processing
	s.Start()
	defer s.Stop()

	s.Suffix = " compacting parquet files"

	// traverse the directory and compact files
	// initialize counters
	totalFiles := 0
	compactedCount := 0

	// traverse the directory and compact files
	err = traverseAndCompact(ctx, db, baseDir, s, &totalFiles, &compactedCount)
	return totalFiles, compactedCount, err

}

func traverseAndCompact(ctx context.Context, db *sql.DB, dirPath string, s *spinner.Spinner, totalFiles *int, compactedCount *int) error {
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
			err := traverseAndCompact(ctx, db, subDirPath, s, totalFiles, compactedCount)
			if err != nil {
				return err
			}
		} else if strings.HasSuffix(entry.Name(), ".parquet") {
			// collect parquet file paths
			parquetFiles = append(parquetFiles, filepath.Join(dirPath, entry.Name()))
		}
	}

	// compact files if there are any `.parquet` files
	if len(parquetFiles) == 0 || len(parquetFiles) == 1 && strings.Contains(parquetFiles[0], compactedFileName) {
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

	// update totals via pointers
	*totalFiles += len(parquetFiles)
	*compactedCount++

	// update the spinner suffix
	if s != nil {
		s.Suffix = fmt.Sprintf(" compacting parquet files (%d files -> %d files)", *totalFiles, *compactedCount)
	}

	return nil
}

func compactParquetFiles(ctx context.Context, db *sql.DB, parquetFiles []string, inputPath string) error {
	// define temp and output file paths
	tempOutputFile := filepath.Join(inputPath, compactedFileName+".tmp")
	outputFile := filepath.Join(inputPath, compactedFileName)

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

	// delete source parquet files
	for _, file := range parquetFiles {
		if err := os.Remove(file); err != nil {
			return fmt.Errorf("failed to delete parquet file %s: %w", file, err)
		}
	}

	// rename temp file to final output file
	if err := os.Rename(tempOutputFile, outputFile); err != nil {
		return fmt.Errorf("failed to rename temp file %s to %s: %w", tempOutputFile, outputFile, err)
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
