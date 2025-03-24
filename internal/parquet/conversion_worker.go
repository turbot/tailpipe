package parquet

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	sdkconstants "github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/filepaths"
)

type parquetJob struct {
	chunkNumber int64
}

// conversionWorker is an implementation of worker that converts JSONL files to Parquet
type conversionWorker struct {
	// channel to receive jobs from the writer
	jobChan chan *parquetJob

	// the parent converter
	converter *Converter

	// source file location
	sourceDir string
	// dest file location
	destDir string

	// helper struct which provides unique filename roots
	fileRootProvider *FileRootProvider
	db               *database.DuckDb
}

func newParquetConversionWorker(converter *Converter) (*conversionWorker, error) {
	w := &conversionWorker{
		jobChan:          converter.jobChan,
		sourceDir:        converter.sourceDir,
		destDir:          converter.destDir,
		fileRootProvider: converter.fileRootProvider,
		converter:        converter,
	}

	// create a new DuckDB instance
	db, err := database.NewDuckDb(database.WithDuckDbExtensions(constants.DuckDbExtensions))
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB wrapper: %w", err)
	}
	w.db = db
	return w, nil
}

// this is the worker function run by all workers, which all read from the ParquetJobPool channel
func (w *conversionWorker) start(ctx context.Context) {
	slog.Debug("worker start")
	// this function runs as long as the worker is running

	// ensure to close on exit
	defer w.close()

	// loop until we are closed
	for {
		select {
		case <-ctx.Done():
			// we are done
			return
		case job := <-w.jobChan:
			if job == nil {
				// we are done
				return
			}
			if err := w.doJSONToParquetConversion(int(job.chunkNumber)); err != nil {
				// send the error to the converter
				w.converter.addJobErrors(err)
				continue
			}
			// atomically increment the completion count on our converter
			atomic.AddInt32(&w.converter.completionCount, 1)

		}
	}
}

func (w *conversionWorker) close() {
	_ = w.db.Close()
}

func (w *conversionWorker) doJSONToParquetConversion(chunkNumber int) error {
	// ensure we signal the converter when we are done
	defer w.converter.wg.Done()
	startTime := time.Now()

	// build the source filename
	jsonFileName := table.ExecutionIdToFileName(w.converter.id, chunkNumber)
	jsonFilePath := filepath.Join(w.sourceDir, jsonFileName)

	// process the ParquetJobPool
	rowCount, err := w.convertFile(jsonFilePath)
	if err != nil {
		return fmt.Errorf("failed to convert file %s: %w", jsonFilePath, err)
	}

	// update the row count
	w.converter.updateRowCount(rowCount)

	// delete JSON file (configurable?)
	if err := os.Remove(jsonFilePath); err != nil {
		return fmt.Errorf("failed to delete JSONL file %s: %w", jsonFilePath, err)
	}
	activeDuration := time.Since(startTime)
	slog.Debug("converted JSONL to Parquet", "file", jsonFilePath, "duration (ms)", activeDuration.Milliseconds())
	return nil
}

// convert the given jsonl file to parquet
func (w *conversionWorker) convertFile(jsonlFilePath string) (int64, error) {
	partition := w.converter.Partition

	// verify the jsonl file has a .jsonl extension
	if filepath.Ext(jsonlFilePath) != ".jsonl" {
		return 0, fmt.Errorf("invalid file type - conversionWorker only supports JSONL files: %s", jsonlFilePath)
	}
	// verify file exists
	if _, err := os.Stat(jsonlFilePath); os.IsNotExist(err) {
		return 0, fmt.Errorf("file does not exist: %s", jsonlFilePath)
	}

	if err := os.MkdirAll(filepath.Dir(w.destDir), 0755); err != nil {
		return 0, fmt.Errorf("failed to create parquet folder: %w", err)
	}

	// render query
	selectQuery := fmt.Sprintf(w.converter.viewQueryFormat, jsonlFilePath)

	// if the partition includes a filter, add a where clause
	if partition.Filter != "" {
		selectQuery += fmt.Sprintf(" where %s", partition.Filter)
	}
	// Create a query to write to partitioned parquet files

	// get a unique file root
	fileRoot := w.fileRootProvider.GetFileRoot()

	// build a query to export the rows to partitioned parquet files
	// NOTE: we initially write to a file with the extension '.parquet.tmp' - this is to avoid the creation of invalid parquet files
	// in the case of a failure
	// once the conversion is complete we will rename them
	partitionColumns := []string{sdkconstants.TpTable, sdkconstants.TpPartition, sdkconstants.TpIndex, sdkconstants.TpDate}
	exportQuery := fmt.Sprintf(`copy (%s) to '%s' (
		format parquet,
		partition_by (%s),
		return_files true,
		overwrite_or_ignore,
		filename_pattern '%s_{i}',
		file_extension '%s'
);
	);`,
		selectQuery,
		w.destDir,
		strings.Join(partitionColumns, ","),
		fileRoot,
		strings.TrimPrefix(filepaths.TempParquetExtension, "."), // no '.' required for duckdb
	)

	row := w.db.QueryRow(exportQuery)
	var rowCount int64
	var files []interface{}
	err := row.Scan(&rowCount, &files)
	if err != nil {
		return 0, fmt.Errorf("error reading files: %w", err)
	}
	slog.Debug("created parquet files", "count", len(files), "files", files)

	// now rename the parquet files
	err = w.renameTempParquetFiles(files)

	return rowCount, err
}

// renameTempParquetFiles renames the given list of temporary parquet files to have a .parquet extension.
// note: we receive the list of files as an interface{} as that is what we read back from the db
func (w *conversionWorker) renameTempParquetFiles(files []interface{}) error {
	var errList []error
	for _, f := range files {
		fileName := f.(string)
		if strings.HasSuffix(fileName, filepaths.TempParquetExtension) {
			newName := strings.TrimSuffix(fileName, filepaths.TempParquetExtension) + ".parquet"
			if err := os.Rename(fileName, newName); err != nil {
				errList = append(errList, fmt.Errorf("%s: %w", fileName, err))
			}
		}
	}

	if len(errList) > 0 {
		var msg strings.Builder
		msg.WriteString(fmt.Sprintf("Failed to rename %d parquet files:\n", len(errList)))
		for _, err := range errList {
			msg.WriteString(fmt.Sprintf("  - %v\n", err))
		}
		return errors.New(msg.String())
	}

	return nil
}
