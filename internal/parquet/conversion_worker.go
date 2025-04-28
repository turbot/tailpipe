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
	maxMemoryMb      int64
}

func newParquetConversionWorker(converter *Converter, maxMemoryMb int64) (*conversionWorker, error) {
	w := &conversionWorker{
		jobChan:          converter.jobChan,
		sourceDir:        converter.sourceDir,
		destDir:          converter.destDir,
		fileRootProvider: converter.fileRootProvider,
		converter:        converter,
		maxMemoryMb:      maxMemoryMb,
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

func (w *conversionWorker) reopenConnection() error {
	// close existing connection
	w.close()
	// create a new connection
	db, err := database.NewDuckDb(database.WithDuckDbExtensions(constants.DuckDbExtensions))
	if err != nil {
		return fmt.Errorf("failed to reopen DuckDB connection: %w", err)
	}
	w.db = db
	return nil
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
	// if we have an error, return it below
	// update the row count
	w.converter.updateRowCount(rowCount)

	// delete JSON file (configurable?)
	if removeErr := os.Remove(jsonFilePath); removeErr != nil {
		// log the error but don't fail
		slog.Error("failed to delete JSONL file", "file", jsonFilePath, "error", removeErr)
	}
	activeDuration := time.Since(startTime)
	slog.Debug("converted JSONL to Parquet", "file", jsonFilePath, "duration (ms)", activeDuration.Milliseconds())
	// remove the conversion error (if any)
	return err
}

// convert the given jsonl file to parquet
func (w *conversionWorker) convertFile(jsonlFilePath string) (_ int64, err error) {
	// verify the jsonl file has a .jsonl extension
	if filepath.Ext(jsonlFilePath) != ".jsonl" {
		return 0, NewConversionError("invalid file type - conversionWorker only supports .jsonl files", 0, jsonlFilePath)
	}
	// verify file exists
	if _, err := os.Stat(jsonlFilePath); os.IsNotExist(err) {
		return 0, NewConversionError("file does not exist", 0, jsonlFilePath)
	}

	// copy the data from the jsonl file to a temp table
	if err := w.copyChunkToTempTable(jsonlFilePath); err != nil {
		// copyChunkToTempTable will already have called handleSchemaChangeError anf handleConversionError
		return 0, err
	}
	// defer the cleanup of the temp table
	defer func() {
		// TODO benchmark whether dropping the table actually makes any difference to memory pressure
		//  or can we rely on the drop if exists?
		// validateSchema creates the table temp_data - the cleanupQuery drops it
		_, tempTableError := w.db.Exec("drop table if exists temp_data;")
		if tempTableError != nil {
			slog.Error("failed to drop temp table", "error", err)
			// if we do not already have an error return this error
			if err == nil {
				err = tempTableError
			}
		}
	}()

	// now validate the data
	if err := w.validateSchema(jsonlFilePath); err != nil {
		// TODO do a partial conversion???
		// if we have an error, return it
		return 0, err
	}

	// ok now we can do the copy query to write the data in the temp table to parquet files
	// we limit the number of partitions we create per copy query to avoid excessive memory usage
	// TODO consider dynamically changing the number of partitions to process based on column count
	const defaultMaxPartitionsPerConversion = 500
	partitionsPerConversion := defaultMaxPartitionsPerConversion

	// get distinct partition key combinations
	partitionKeys, err := w.getDistinctPartitionKeyCombinations()
	if err != nil {
		return 0, handleConversionError(err, jsonlFilePath)
	}
	slog.Debug("found partition combinations", "count", len(partitionKeys))

	// Process partition keys in batches
	var totalRowCount int64
	for len(partitionKeys) > 0 {
		batchSize := partitionsPerConversion
		if batchSize > len(partitionKeys) {
			batchSize = len(partitionKeys)
		}

		batch := partitionKeys[:batchSize]
		rowCount, err := w.doConversionForBatch(jsonlFilePath, batch)
		if err != nil {
			if conversionRanOutOfMemory(err) {
				if err := w.reopenConnection(); err != nil {
					return totalRowCount, err
				}
				partitionsPerConversion = partitionsPerConversion / 2
				if partitionsPerConversion < 1 {
					return totalRowCount, fmt.Errorf("failed to convert batch - partition count reduced to 0")
				}
				continue
			}
			return totalRowCount, err
		}

		totalRowCount += rowCount
		partitionKeys = partitionKeys[batchSize:]
	}

	return totalRowCount, nil

	// we need to perform validation if:
	// 1. the table schema was partial (i.e. this is a custom table with RowMappings defined)
	// 2. this is a directly converted artifact
	// 3. any required columns have transforms

	//selectQuery, cleanupQuery, rowValidationError = w.validateSchema(jsonlFilePath, selectQuery, columnsToValidate)
	//
	//if cleanupQuery != "" {
	//	defer func() {
	//		// TODO benchmark whether dropping the table actually makes any difference to memory pressure
	//		//  or can we rely on the drop if exists?
	//		// validateSchema creates the table temp_data - the cleanupQuery drops it
	//		_, tempTableError := w.db.Exec(cleanupQuery)
	//		if tempTableError != nil {
	//			slog.Error("failed to drop temp table", "error", err)
	//			// if we do not already have an error return this error
	//			if err == nil {
	//				err = tempTableError
	//			}
	//		}
	//	}()
	//}
	//// did validation fail
	//if rowValidationError != nil {
	//	// if no select query was returned, we cannot proceed - just return the error
	//	if selectQuery == "" {
	//		return 0, rowValidationError
	//	}
	//	// so we have a validation error - but we DID return an updated select query - that indicates we should
	//	// continue and just report the row validation errors
	//
	//	// ensure that we return the row validation error, merged with any other error we receive
	//	defer func() {
	//		if err == nil {
	//			err = rowValidationError
	//		} else {
	//			var conversionError *ConversionError
	//			if errors.As(rowValidationError, &conversionError) {
	//				// we have a conversion error - we need to set the row count to 0
	//				// so we can report the error
	//				conversionError.Merge(err)
	//			}
	//			err = conversionError
	//		}
	//	}()
	//}

}

func conversionRanOutOfMemory(err error) bool {
	// TODO implement a better check for out of memory errors
	return true
}

func (w *conversionWorker) copyChunkToTempTable(jsonlFilePath string) error {
	var queryBuilder strings.Builder

	// render the view query with the jsonl file path
	// - this build a select clause which selects the required data from the JSONL file (with columns types specified)
	selectQuery := fmt.Sprintf(w.converter.viewQueryFormat, jsonlFilePath)

	// now copy the data from the JSONL file to a temp table

	// drop the temp table if it exists
	queryBuilder.WriteString("drop table if exists temp_data;\n")

	// create the temp table and copy the data from the JSONL file
	// note:
	// - also select a row id
	// - add extra select wrapper to allow for wrapping query before filter is applied so filter can use struct fields with dot-notation
	queryBuilder.WriteString(fmt.Sprintf("create temp table temp_data as select row_number() over () as rowid, * from (%s);\n", selectQuery))
	_, err := w.db.Exec(queryBuilder.String())
	if err != nil {
		return w.handleSchemaChangeError(err, jsonlFilePath)

	}
	return nil
}

// getDistinctPartitionKeyCombinations returns a map of partition key column names to their values from temp_data table
func (w *conversionWorker) getDistinctPartitionKeyCombinations() ([]map[string]interface{}, error) {
	// define columns
	columns := []string{sdkconstants.TpTable, sdkconstants.TpPartition, sdkconstants.TpIndex, sdkconstants.TpDate}

	// build query to get distinct combinations
	query := fmt.Sprintf(`select distinct %s from temp_data`,
		strings.Join(columns, ","))

	// execute query
	rows, err := w.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, 4)
		scanArgs := make([]interface{}, 4)
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}

		// create map for this row
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			rowMap[col] = values[i]
		}
		result = append(result, rowMap)
	}
	return result, rows.Err()
}

func (w *conversionWorker) doConversionForBatch(jsonlFilePath string, partitionKeys []map[string]interface{}) (int64, error) {
	// Create a query to write to partitioned parquet files

	// get a unique file root
	fileRoot := w.fileRootProvider.GetFileRoot()
	selectQuery := fmt.Sprintf("select * from temp_data")
	if w.converter.Partition.Filter != "" {
		selectQuery += fmt.Sprintf(" where %s", w.converter.Partition.Filter)
	}

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
	err := row.Scan(&rowCount, &files)
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
// 1. if the format is one which supports direct artifact-JSONL conversion (i.e. jsonl, delimited) we must validate all required columns
// 2. if any required columns have transforms, we must validate those columns as the transforms are applied at this stage
// TODO once we have tested/benchmarked, move all validation here and do not validate in plugin even for static tables
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

// validateSchema copies the data from the given select query to a temp table and validates required fields are non null
// it also validates that the schema of the chunk is the same as the inferred schema and if it is not, reports a useful error
// the query count of invalid rows and a list of null fields
func (w *conversionWorker) validateSchema(jsonlFilePath string) error {
	// (NOTE: if we validate we write the data to a temp table then update the select query to read from that temp table)
	// get list of columns to validate
	columnsToValidate := w.getColumnsToValidate()

	// if we have no columns to validate, biuld a  validation query to return the number of invalid rows and the columns with nulls
	validationQuery := w.buildValidationQuery(columnsToValidate)

	row := w.db.QueryRow(validationQuery)
	var totalRows int64
	var columnsWithNullsInterface []interface{}

	err := row.Scan(&totalRows, &columnsWithNullsInterface)
	if err != nil {
		return w.handleSchemaChangeError(err, jsonlFilePath)
	}

	// Convert the interface slice to string slice
	var columnsWithNulls []string
	for _, col := range columnsWithNullsInterface {
		if col != nil {
			columnsWithNulls = append(columnsWithNulls, col.(string))
		}
	}

	var rowValidationError error
	if totalRows > 0 {
		// we have a failure - return an error with details about which columns had nulls
		nullColumns := "unknown"
		if len(columnsWithNulls) > 0 {
			nullColumns = strings.Join(columnsWithNulls, ", ")
		}
		// if we have row validation errors, we return a ConversionError but also return the select query
		// so the calling code can convert the rest of the chunk
		rowValidationError = NewConversionError(fmt.Sprintf("validation failed - found null values in columns: %s", nullColumns), totalRows, jsonlFilePath)

		// delete invalid rows from the temp table
		if err := w.deleteInvalidRows(columnsToValidate); err != nil {
			return handleConversionError(err, jsonlFilePath)
		}
	}
	// if we have no errors, we can just return nil
	return nil
}

// handleSchemaChangeError determines if the error is because the schema of this chunk is different to the inferred schema
// infer the schema of this chunk and compare - if they are different, return that in an error
func (w *conversionWorker) handleSchemaChangeError(err error, jsonlFilePath string) error {
	schemaChangeErr := w.converter.detectSchemaChange(jsonlFilePath)
	if schemaChangeErr != nil {
		// if the error returned from detectSchemaChange is a SchemaChangeError, return that instead of the original error
		var e = &SchemaChangeError{}
		if errors.As(schemaChangeErr, &e) {
			// update err and fall through to handleConversionError - this wraps the error with additional row count info
			err = e
		}
	}

	// just return the original error, wrapped with the row count
	return handleConversionError(err, jsonlFilePath)
}

// buildValidationQuery builds a query to copy the data from the select query to a temp table
// it then validates that the required columns are not null, removing invalid rows and returning
// the count of invalid rows and the columns with nulls
func (w *conversionWorker) buildValidationQuery(columnsToValidate []string) string {
	queryBuilder := strings.Builder{}

	// Build the validation query that:
	// - Counts distinct rows that have null values in required columns
	// - Lists all required columns that contain null values
	queryBuilder.WriteString(`select
    count(distinct rowid) as rows_with_required_nulls,  -- Count unique rows with nulls in required columns
    coalesce(list(distinct col), []) as required_columns_with_nulls  -- List required columns that have null values, defaulting to empty list if NULL
from (`)

	// Step 3: For each required column we need to validate:
	// - Create a query that selects rows where this column is null
	// - Include the column name so we know which column had the null
	// - UNION ALL combines all these results (faster than UNION as we don't need to deduplicate)
	for i, col := range columnsToValidate {
		if i > 0 {
			queryBuilder.WriteString("    union all\n")
		}
		// For each required column, create a query that:
		// - Selects the rowid (to count distinct rows)
		// - Includes the column name (to list which columns had nulls)
		// - Only includes rows where this column is null
		queryBuilder.WriteString(fmt.Sprintf("    select rowid, '%s' as col from temp_data where %s is null\n", col, col))
	}

	queryBuilder.WriteString(");")

	return queryBuilder.String()
}

// buildNullCheckQuery builds a WHERE clause to check for null values in the specified columns
func (w *conversionWorker) buildNullCheckQuery(columnsToValidate []string) string {
	if len(columnsToValidate) == 0 {
		return ""
	}

	// build a slice of null check conditions
	conditions := make([]string, len(columnsToValidate))
	for i, col := range columnsToValidate {
		conditions[i] = fmt.Sprintf("%s is null", col)
	}
	return strings.Join(conditions, " or ")
}

// deleteInvalidRows removes rows with null values in the specified columns from the temp table
func (w *conversionWorker) deleteInvalidRows(columnsToValidate []string) error {
	if len(columnsToValidate) == 0 {
		return nil
	}

	whereClause := w.buildNullCheckQuery(columnsToValidate)
	query := fmt.Sprintf("delete from temp_data where %s;", whereClause)

	_, err := w.db.Exec(query)
	return err
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
