package parquet

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/table"
	"github.com/turbot/tailpipe/internal/constants"
)

type simpleParquetJob struct {
	chunkNumber int64
}

// parquetConversionWorkerSimple is an implementation of worker that converts JSONL files to Parquet
type parquetConversionWorkerSimple struct {
	// channel to receive jobs from the writer
	jobChan chan *simpleParquetJob

	// the parent converter
	converter *ParquetConverter

	// source file location
	sourceDir string
	// dest file location
	destDir string

	// helper struct which provides unique filename roots
	fileRootProvider *FileRootProvider
	db               *duckDb
}

func newParquetConversionWorkerSimple(converter *ParquetConverter) (*parquetConversionWorkerSimple, error) {
	w := &parquetConversionWorkerSimple{
		jobChan:          converter.jobChan,
		sourceDir:        converter.sourceDir,
		destDir:          converter.destDir,
		fileRootProvider: converter.fileRootProvider,
		converter:        converter,
	}

	// create a new DuckDB instance
	db, err := newDuckDb()
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB wrapper: %w", err)
	}
	w.db = db
	return w, nil
}

// this is the worker function run by all workers, which all read from the ParquetJobPool channel
func (w *parquetConversionWorkerSimple) start(ctx context.Context) {
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

func (w *parquetConversionWorkerSimple) close() {
	_ = w.db.Close()
}

func (w *parquetConversionWorkerSimple) doJSONToParquetConversion(chunkNumber int) error {
	slog.Info("converting JSONL to Parquet", "chunk", chunkNumber)
	defer slog.Info("finished converting JSONL to Parquet", "chunk", chunkNumber)

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
	atomic.AddInt64(&w.converter.rowCount, rowCount)

	// delete JSON file (configurable?)
	if err := os.Remove(jsonFilePath); err != nil {
		return fmt.Errorf("failed to delete JSONL file %s: %w", jsonFilePath, err)
	}
	activeDuration := time.Since(startTime)
	slog.Debug("converted JSONL to Parquet", "file", jsonFilePath, "duration (ms)", activeDuration.Milliseconds())
	return nil
}

// convert the given jsonl file to parquet
func (w *parquetConversionWorkerSimple) convertFile(jsonlFilePath string) (int64, error) {
	partition := w.converter.Partition

	// verify the jsonl file has a .jsonl extension
	if filepath.Ext(jsonlFilePath) != ".jsonl" {
		return 0, fmt.Errorf("invalid file type - parquetConversionWorkerSimple only supports JSONL files: %s", jsonlFilePath)
	}
	// verify file exists
	if _, err := os.Stat(jsonlFilePath); os.IsNotExist(err) {
		return 0, fmt.Errorf("file does not exist: %s", jsonlFilePath)
	}

	// determine the root based on the partition

	if err := os.MkdirAll(filepath.Dir(w.destDir), 0755); err != nil {
		return 0, fmt.Errorf("failed to create parquet folder: %w", err)
	}

	// render query
	selectQuery := fmt.Sprintf(w.converter.viewQueryFormat, jsonlFilePath)

	// if the partition includes a filter, add a WHERE clause
	if partition.Filter != "" {
		selectQuery += fmt.Sprintf(" WHERE %s", partition.Filter)
	}
	// Create a query to write to partitioned parquet files

	// get a unique file root
	fileRoot := w.fileRootProvider.GetFileRoot()

	partitionColumns := []string{constants.TpTable, constants.TpPartition, constants.TpIndex, constants.TpDate}
	exportQuery := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET, PARTITION_BY (%s), OVERWRITE_OR_IGNORE, FILENAME_PATTERN "%s_{i}");`,
		selectQuery, w.destDir, strings.Join(partitionColumns, ","), fileRoot)

	_, err := w.db.Exec(exportQuery)
	if err != nil {
		return 0, fmt.Errorf("conversion query failed: %w", err)
	}

	// now read row count
	count, err := getRowCount(w.db.DB, w.destDir, fileRoot, partition.TableName)
	if err != nil {
		slog.Warn("failed to get row count - conversion failed", "error", err, "query", exportQuery)
	}
	return count, err

}
