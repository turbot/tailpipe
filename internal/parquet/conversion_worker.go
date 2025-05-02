package parquet

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/marcboeker/go-duckdb/v2"
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
	fileRootProvider              *FileRootProvider
	db                            *database.DuckDb
	maxMemoryMb                   int
	maxPartitionKeysPerConversion int
}

func newConversionWorker(converter *Converter, maxMemoryMb int) (*conversionWorker, error) {

	w := &conversionWorker{
		jobChan:                       converter.jobChan,
		sourceDir:                     converter.sourceDir,
		destDir:                       converter.destDir,
		fileRootProvider:              converter.fileRootProvider,
		converter:                     converter,
		maxMemoryMb:                   maxMemoryMb,
		maxPartitionKeysPerConversion: getMaxPartitionsPerConversion(maxMemoryMb),
	}

	if err := w.validate(); err != nil {
		return nil, err
	}
	if err := w.createDuckDbConnection(); err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}

	return w, nil
}

func getMaxPartitionsPerConversion(maxMemoryMb int) int {
	// 8Gb mem -> 500
	// 4Gb mem -> 250

	// scale the number of partitions inversely with memory size
	// use 8GB as the baseline (1000 partitions)
	baselineMemoryMb := 8 * 1024
	baselinePartitions := 500

	// calculate scaled partitions (more memory = fewer partitions)
	scaledPartitions := baselinePartitions * baselineMemoryMb / maxMemoryMb

	defer slog.Info("getMaxPartitionsPerConversion", "worker max memory (mb)", maxMemoryMb, "max hive partition keys per conversion", scaledPartitions)

	// ensure we stay within reasonable bounds
	minPartitions := 100
	maxPartitions := 1000

	if scaledPartitions < minPartitions {
		return minPartitions
	}
	if scaledPartitions > maxPartitions {
		return maxPartitions
	}
	return scaledPartitions
}

// validate our params
func (w *conversionWorker) validate() error {
	maxAllowedMemoryMB := 256 * 1024 // 256GB in MB
	if w.maxMemoryMb < 0 || w.maxMemoryMb > maxAllowedMemoryMB {
		return fmt.Errorf("memory must be between 0 and %d MB, got %d", maxAllowedMemoryMB, w.maxMemoryMb)
	}
	return nil
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
			w.converter.updateCompletionCount(1)

		}
	}
}

func (w *conversionWorker) close() {
	_ = w.db.Close()
}

// createDuckDbConnection creates a new DuckDB connection, setting the max memory limit
func (w *conversionWorker) createDuckDbConnection() error {
	db, err := database.NewDuckDb(database.WithDuckDbExtensions(constants.DuckDbExtensions), database.WithMaxMemoryMb(w.maxMemoryMb))
	if err != nil {
		return fmt.Errorf("failed to reopen DuckDB connection: %w", err)
	}
	w.db = db
	return nil
}

func (w *conversionWorker) forceMemoryRelease() error {
	// we need to flush the memory to release it - do this by setting a low memory limit then the full one
	// NOTE: do not set the memory to zero as we have temp table data
	const minMemoryMb = 64

	// Set to minimum memory - note the use of ? parameter
	if _, err := w.db.Exec("PRAGMA memory_limit=?", minMemoryMb); err != nil {
		return fmt.Errorf("memory flush failed: %w", err)
	}

	// Reset to configured memory limit
	if _, err := w.db.Exec("PRAGMA memory_limit=?", w.maxMemoryMb); err != nil {
		return fmt.Errorf("memory reset failed: %w", err)
	}
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
	err := w.convertFile(jsonFilePath)

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
func (w *conversionWorker) convertFile(jsonlFilePath string) (err error) {
	// verify the jsonl file has a .jsonl extension
	if filepath.Ext(jsonlFilePath) != ".jsonl" {
		return NewConversionError(errors.New("invalid file type - conversionWorker only supports .jsonl files"), 0, jsonlFilePath)
	}
	// verify file exists
	if _, err := os.Stat(jsonlFilePath); os.IsNotExist(err) {
		return NewConversionError(errors.New("file does not exist"), 0, jsonlFilePath)
	}

	// copy the data from the jsonl file to a temp table
	if err := w.copyChunkToTempTable(jsonlFilePath); err != nil {
		// copyChunkToTempTable will already have called handleSchemaChangeError anf handleConversionError
		return err
	}
	// defer the cleanup of the temp table
	defer func() {
		// TODO benchmark whether dropping the table actually makes any difference to memory pressure
		//  or can we rely on the drop if exists?
		// validateRows creates the table temp_data - the cleanupQuery drops it
		_, tempTableError := w.db.Exec("drop table if exists temp_data;")
		if tempTableError != nil {
			slog.Error("failed to drop temp table", "error", tempTableError)
			// if we do not already have an error return this error
			if err == nil {
				err = tempTableError
			}
		}
	}()

	// now validate the data
	if validateRowsError := w.validateRows(jsonlFilePath); err != nil {
		// if the error is NOT RowValidationError, just return it
		if !errors.Is(validateRowsError, &RowValidationError{}) {
			return handleConversionError(validateRowsError, jsonlFilePath)
		}

		// so it IS a row validation error - the invalid rows will have been removed from the temp table
		// - process the rest of the chunk
		// ensure that we return the row validation error, merged with any other error we receive
		defer func() {
			if err == nil {
				err = validateRowsError
			} else {
				var conversionError *ConversionError
				if errors.As(validateRowsError, &conversionError) {
					// we have a conversion error - we need to set the row count to 0
					// so we can report the error
					conversionError.Merge(err)
				}
				err = conversionError
			}
		}()
	}

	// ok now we can do the copy query to write the data in the temp table to parquet files
	// we limit the number of partitions we create per copy query to avoid excessive memory usage
	// TODO consider dynamically changing the number of partitions to process based on column count

	partitionsPerConversion := w.maxPartitionKeysPerConversion

	// get row counts for each distinct partition
	partitionRowCounts, err := w.getPartitionRowCounts()
	if err != nil {
		return handleConversionError(err, jsonlFilePath)
	}
	slog.Debug("found partition combinations", "count", len(partitionRowCounts))

	// Process partitions in batches using row offsets.
	//
	// For each batch:
	// - Calculate how many partitions to include (up to partitionsPerConversion)
	// - Sum the row counts for the selected partitions to determine how many rows to process
	// - Export the corresponding rows to Parquet based on rowid range
	//
	// If an out-of-memory error occurs during export:
	// - Reopen the DuckDB connection
	// - Halve the number of partitions processed per batch
	// - Retry processing
	var (
		totalRowCount int64
		rowOffset     int64
	)

	for len(partitionRowCounts) > 0 {
		batchSize := partitionsPerConversion
		if batchSize > len(partitionRowCounts) {
			batchSize = len(partitionRowCounts)
		}

		// Calculate total number of rows to process for this batch
		var rowsInBatch int64
		for i := 0; i < batchSize; i++ {
			rowsInBatch += partitionRowCounts[i]
		}

		// Perform conversion for this batch using rowid ranges
		rowCount, err := w.doConversionForBatch(jsonlFilePath, rowOffset, rowsInBatch)
		if err != nil {
			if conversionRanOutOfMemory(err) {
				// If out of memory, fluish memory, reopen the connection, and retry with fewer partitions
				if err := w.forceMemoryRelease(); err != nil {
					return err
				}
				partitionsPerConversion /= 2
				if partitionsPerConversion < 1 {
					return fmt.Errorf("failed to convert batch - partition count reduced to 0")
				}
				slog.Debug("restarting conversion with fewer partitions", "file", jsonlFilePath, "partitions", partitionsPerConversion)
				continue
			}
			return err
		}

		// Update counters and advance to the next batch
		totalRowCount += rowCount
		rowOffset += rowsInBatch
		partitionRowCounts = partitionRowCounts[batchSize:]
		// if we have an error, return it below
		// update the row count
		w.converter.updateRowCount(rowCount)

	}

	return nil
}

// conversionRanOutOfMemory checks if the error is an out-of-memory error from DuckDB
func conversionRanOutOfMemory(err error) bool {
	var duckDBErr = &duckdb.Error{}
	if errors.As(err, &duckDBErr) {
		return duckDBErr.Type == duckdb.ErrorTypeOutOfMemory
	}
	return false
}

func (w *conversionWorker) copyChunkToTempTable(jsonlFilePath string) error {
	var queryBuilder strings.Builder

	// render the view query with the jsonl file path
	// - this build a select clause which selects the required data from the JSONL file (with columns types specified)
	selectQuery := fmt.Sprintf(w.converter.viewQueryFormat, jsonlFilePath)

	// Step: Prepare the temp table from JSONL input
	//
	// - Drop the temp table if it exists
	// - Create a new temp table by reading from the JSONL file
	// - Add a row ID (row_number) for stable ordering and chunking
	// - Wrap the original select query to allow dot-notation filtering on nested structs later
	// - Sort the data by partition key columns (only tp_index, tp_date - there will only be a single table and partition)
	// so that full partitions can be selected using only row offsets (because partitions are stored contiguously)
	queryBuilder.WriteString(fmt.Sprintf(`
drop table if exists temp_data;

create temp table temp_data as
select
  row_number() over (order by tp_index, tp_date) as rowid,
  *
from (
  %s
)
order by
  tp_index, tp_date;
`, selectQuery))
	_, err := w.db.Exec(queryBuilder.String())
	if err != nil {
		return w.handleSchemaChangeError(err, jsonlFilePath)

	}
	return nil
}

// getPartitionRowCounts returns a slice of row counts,
// where each count corresponds to a distinct combination of partition key columns
// (tp_table, tp_partition, tp_index, tp_date) in the temp_data table.
//
// The counts are ordered by the partition key columns to allow us to efficiently select
// full partitions based on row offsets without needing additional filtering.
func (w *conversionWorker) getPartitionRowCounts() ([]int64, error) {
	// get the distinct partition key combinations
	partitionColumns := []string{sdkconstants.TpTable, sdkconstants.TpPartition, sdkconstants.TpIndex, sdkconstants.TpDate}
	partitionColumnsString := strings.Join(partitionColumns, ",")

	query := fmt.Sprintf(`
		select count(*) as row_count
		from temp_data
		group by %s
		order by %s
	`, partitionColumnsString, partitionColumnsString)

	rows, err := w.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []int64
	for rows.Next() {
		var count int64
		if err := rows.Scan(&count); err != nil {
			return nil, err
		}
		result = append(result, count)
	}
	return result, rows.Err()
}

// doConversionForBatch writes a batch of rows from the temp_data table to partitioned Parquet files.
//
// It selects rows based on rowid, using the provided startRowId and rowCount to control the range:
// - Rows with rowid > startRowId and rowid <= (startRowId + rowCount) are selected.
//
// This approach ensures that full partitions are processed contiguously and allows efficient batching
// without needing complex WHERE clauses.
//
// Returns the number of rows written and any error encountered.
func (w *conversionWorker) doConversionForBatch(jsonlFilePath string, startRowId int64, rowCount int64) (int64, error) {
	// Create a query to write a batch of rows to partitioned Parquet files

	// Get a unique file root
	fileRoot := w.fileRootProvider.GetFileRoot()

	// Build select query to pick the correct rows
	selectQuery := fmt.Sprintf(`
		select *
		from temp_data
		where rowid > %d and rowid <= %d
	`, startRowId, startRowId+rowCount)

	// Build the export query
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
		strings.TrimPrefix(filepaths.TempParquetExtension, "."),
	)

	// Execute the export
	row := w.db.QueryRow(exportQuery)
	var exportedRowCount int64
	var files []interface{}
	err := row.Scan(&exportedRowCount, &files)
	if err != nil {
		return 0, handleConversionError(err, jsonlFilePath)
	}
	slog.Debug("created parquet files", "count", len(files))

	// Rename temporary Parquet files
	err = w.renameTempParquetFiles(files)
	return exportedRowCount, err
}

func (w *conversionWorker) buildSelectClause(partitionKeys []map[string]interface{}) string {

	var whereConditions []string
	for _, keyMap := range partitionKeys {
		var conditions []string
		for col, val := range keyMap {
			if val == nil {
				conditions = append(conditions, fmt.Sprintf("%s is null", col))
			} else {
				conditions = append(conditions, fmt.Sprintf("%s = %v", col, val))
			}
		}
		if len(conditions) > 0 {
			whereConditions = append(whereConditions, "("+strings.Join(conditions, " and ")+")")
		}
	}
	selectQuery := fmt.Sprintf("select * from temp_data")
	if len(whereConditions) > 0 {
		selectQuery += " where " + strings.Join(whereConditions, " or ")
	}

	if w.converter.Partition.Filter != "" {
		if len(whereConditions) > 0 {
			selectQuery += fmt.Sprintf(" and %s", w.converter.Partition.Filter)
		} else {
			selectQuery += fmt.Sprintf(" where %s", w.converter.Partition.Filter)
		}
	}
	return selectQuery
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

	// Added nil check protection
	if w.converter.conversionSchema == nil {
		return res
	}

	// otherwise validate required columns which have a transform
	for _, col := range w.converter.conversionSchema.Columns {
		if col.Required && (col.Transform != "" || formatSupportsDirectConversion) {
			res = append(res, col.ColumnName)
		}
	}
	return res
}

// validateRows copies the data from the given select query to a temp table and validates required fields are non null
// it also validates that the schema of the chunk is the same as the inferred schema and if it is not, reports a useful error
// the query count of invalid rows and a list of null fields
func (w *conversionWorker) validateRows(jsonlFilePath string) error {
	// (NOTE: if we validate we write the data to a temp table then update the select query to read from that temp table)
	// get list of columns to validate
	columnsToValidate := w.getColumnsToValidate()
	if len(columnsToValidate) == 0 {
		// no columns to validate - we are done
		return nil
	}

	// if we have no columns to validate, biuld a  validation query to return the number of invalid rows and the columns with nulls
	validationQuery := w.buildValidationQuery(columnsToValidate)

	row := w.db.QueryRow(validationQuery)
	var failedRowCount int64
	var columnsWithNullsInterface []interface{}

	err := row.Scan(&failedRowCount, &columnsWithNullsInterface)
	if err != nil {
		return w.handleSchemaChangeError(err, jsonlFilePath)
	}

	if failedRowCount == 0 {
		// no rows with nulls - we are done
		return nil
	}

	// delete invalid rows from the temp table
	if err := w.deleteInvalidRows(columnsToValidate); err != nil {
		// failed to delete invalid rows - return an error
		return handleConversionError(err, jsonlFilePath)
	}

	// Convert the interface slice to string slice
	var columnsWithNulls []string
	for _, col := range columnsWithNullsInterface {
		if col != nil {
			columnsWithNulls = append(columnsWithNulls, col.(string))
		}
	}

	// we have a failure - return an error with details about which columns had nulls
	return NewConversionError(NewRowValidationError(columnsWithNulls), failedRowCount, jsonlFilePath)
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
