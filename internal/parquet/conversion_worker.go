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
	chunkNumber int32
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

	// create a new DuckDB instance for each worker
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
			if err := w.doJSONToParquetConversion(job.chunkNumber); err != nil {
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

func (w *conversionWorker) doJSONToParquetConversion(chunkNumber int32) error {
	// ensure we signal the converter when we are done
	defer w.converter.wg.Done()
	startTime := time.Now()

	// build the source filename
	jsonFileName := table.ExecutionIdToJsonlFileName(w.converter.id, chunkNumber)
	jsonFilePath := filepath.Join(w.sourceDir, jsonFileName)

	// process the ParquetJobPool
	rowCount, err := w.convertFile(jsonFilePath)
	if err != nil {
		// don't wrap error already a ConversionError
		return err
	}

	// update the row count
	w.converter.updateRowCount(rowCount)

	// delete JSON file (configurable?)
	if err := os.Remove(jsonFilePath); err != nil {
		// log the error but don't fail
		slog.Error("failed to delete JSONL file", "file", jsonFilePath, "error", err)
	}
	activeDuration := time.Since(startTime)
	slog.Debug("converted JSONL to Parquet", "file", jsonFilePath, "duration (ms)", activeDuration.Milliseconds())
	return nil
}

// convert the given jsonl file to parquet
func (w *conversionWorker) convertFile(jsonlFilePath string) (_ int64, err error) {
	partition := w.converter.Partition

	// verify the jsonl file has a .jsonl extension
	if filepath.Ext(jsonlFilePath) != ".jsonl" {
		return 0, NewConversionError("invalid file type - conversionWorker only supports .jsonl files", 0, jsonlFilePath)
	}
	// verify file exists
	if _, err := os.Stat(jsonlFilePath); os.IsNotExist(err) {
		return 0, NewConversionError("file does not exist", 0, jsonlFilePath)
	}

	// render the view query
	selectQuery := fmt.Sprintf(w.converter.viewQueryFormat, jsonlFilePath)

	// if the partition includes a filter, add a where clause
	if partition.Filter != "" {
		selectQuery += fmt.Sprintf(" where %s", partition.Filter)
	}

	// if this is a directly converted artifact, or of any required columns have transforms, we need to validate the data
	// (NOTE: if we validate we write the data to a temp table then update the select query to read from that temp table)
	// get list of columns to validate
	columnsToValidate := w.getColumnsToValidate()
	if len(columnsToValidate) > 0 {
		updatedSelectQuery, cleanupQuery, err := w.validateRequiredFields(jsonlFilePath, selectQuery, columnsToValidate)
		if err != nil {
			return 0, err
		}
		selectQuery = updatedSelectQuery
		defer func() {
			// TODO benchmark whether dropping the table actually makes any difference to memory pressure
			//  or can we rely on the drop if exists?
			// validateRequiredFields creates the table temp_data - the cleanupQuery drops it
			_, tempTableError := w.db.Exec(cleanupQuery)
			if tempTableError != nil {
				slog.Error("failed to drop temp table", "error", err)
				// if we do not already have an error return this error
				if err == nil {
					err = tempTableError
				}
			}
		}()
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
	err = row.Scan(&rowCount, &files)
	if err != nil {
		// try to get the row count of the file we failed to convert
		return 0, handleConversionError(err, jsonlFilePath)

	}
	slog.Debug("created parquet files", "count", len(files))

	// now rename the parquet files
	err = w.renameTempParquetFiles(files)

	return rowCount, err
}

// getColumnsToValidate returns the list of columns which need to be validated at this staghe
// normally, required columns are validated in the plugin, however there are a couple of exceptions:
// 1. if the format is one which supports direct artifcact-JSONL conversion (i.e. jsonl, delimited) we must validate all required columns
// 2. if any required columns have transforms, we must validate those columns as the transforms are applied at this stage
func (w *conversionWorker) getColumnsToValidate() []string {
	var res []string
	// if the format is one which requires a direct conversion we must validate all required columns
	formatSupportsDirectConversion := w.converter.Partition.FormatSupportsDirectConversion()

	// otherwise validate required columns which have a transform
	for _, col := range w.converter.conversionSchema.Columns {
		if col.Required && (col.Transform != "" || formatSupportsDirectConversion) {
			res = append(res, col.ColumnName)
		}
	}
	return res
}

// validateRequiredFields copys the data from the given select query to a temp table and validates required fields are non null
// the query  count of invalid rows and a list of null fields
func (w *conversionWorker) validateRequiredFields(jsonlFilePath string, selectQuery string, columnsToValidate []string) (string, string, error) {
	validationQuery := w.buildValidationQuery(selectQuery, columnsToValidate)

	row := w.db.QueryRow(validationQuery)
	var totalRows int64
	var columnsWithNulls *string // Use pointer to string to handle NULL values

	err := row.Scan(&totalRows, &columnsWithNulls)
	if err != nil {
		// try to get the row count of the file we failed to convert
		return "", "", handleConversionError(err, jsonlFilePath)
	}
	if totalRows > 0 {
		// we have a failure - return an error with details about which columns had nulls
		nullColumns := "unknown"
		if columnsWithNulls != nil {
			nullColumns = *columnsWithNulls
		}
		return "", "", NewConversionError(fmt.Sprintf("validation failed - found null values in columns: %s", nullColumns), totalRows, jsonlFilePath)
	}
	selectQuery = fmt.Sprintf("select * from temp_data")
	cleanupQuery := fmt.Sprintf("drop table temp_data")
	return selectQuery, cleanupQuery, nil
}

func (w *conversionWorker) buildValidationQuery(selectQuery string, columnsToValidate []string) string {
	queryBuilder := strings.Builder{}
	queryBuilder.WriteString("drop table if exists temp_data;\n")
	queryBuilder.WriteString(fmt.Sprintf("create temp table temp_data as %s;\n", selectQuery))
	queryBuilder.WriteString(`select
    count(*) as total_rows,
    list(distinct col) as columns_with_nulls
from (`)

	if len(columnsToValidate) > 0 {
		queryBuilder.WriteString("\n")
		for i, col := range columnsToValidate {
			if i > 0 {
				queryBuilder.WriteString("\n    union all\n")
			}
			queryBuilder.WriteString(fmt.Sprintf("    select '%s' as col from temp_data where %s is null", col, col))
		}
	}
	queryBuilder.WriteString("\n)\n")
	validationQuery := queryBuilder.String()
	return validationQuery
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
