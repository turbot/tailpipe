package parquet

import (
	"database/sql"
	"fmt"
	"github.com/turbot/tailpipe/internal/filepaths"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
)

// parquetConversionWorker is an implementation of worker that converts JSONL files to Parquet
type parquetConversionWorker struct {
	// channel to receive jobs from the writer
	jobChan chan parquetJob
	// channel to send errors to the writer
	errorChan chan parquetJobError

	// source file location
	sourceDir string
	// dest file location
	destDir string

	// cache partition schemas - key by partition name
	viewQueries map[string]string
	// helper struct which provides unique filename roots
	fileRootProvider *FileRootProvider
	db               *duckDb
}

func newParquetConversionWorker(jobChan chan parquetJob, errorChan chan parquetJobError, sourceDir, destDir string, fileRootProvider *FileRootProvider) (*parquetConversionWorker, error) {
	w := &parquetConversionWorker{
		jobChan:          jobChan,
		errorChan:        errorChan,
		sourceDir:        sourceDir,
		destDir:          destDir,
		viewQueries:      make(map[string]string),
		fileRootProvider: fileRootProvider,
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
func (w *parquetConversionWorker) start() {
	slog.Debug("worker start")

	// loop until we are closed
	for job := range w.jobChan {
		// ok we have a job

		if err := w.doJSONToParquetConversion(job); err != nil {
			slog.Error("worker failed to process job", "error", err)
			// send the error to the writer
			w.errorChan <- parquetJobError{job.groupId, err}
			continue
		}
		// increment the completion count
		atomic.AddInt32(job.completionCount, 1)

		// log the completion count
		if *job.completionCount%100 == 0 {
			slog.Debug("ParquetJobPool completion count", "ParquetJobPool", job.groupId, "count", *job.completionCount)
		}

	}

	// we are done
	w.close()
}

func (w *parquetConversionWorker) close() {
	w.db.Close()
}

func (w *parquetConversionWorker) doJSONToParquetConversion(job parquetJob) error {
	startTime := time.Now()

	// build the source filename
	jsonFileName := table.ExecutionIdToFileName(job.groupId, job.chunkNumber)
	jsonFilePath := filepath.Join(w.sourceDir, jsonFileName)
	s := job.SchemaFunc()
	// process the ParquetJobPool
	rowCount, err := w.convertFile(jsonFilePath, job.Partition, s)
	if err != nil {
		slog.Error("failed to convert file", "error", err)
		return fmt.Errorf("failed to convert file %s: %w", jsonFilePath, err)
	}
	// update the row count
	// increment the completion count
	// HACK - this breaks the abstraction of file job but will do for now
	atomic.AddInt64(job.rowCount, rowCount)

	// delete JSON file (configurable?)
	if err := os.Remove(jsonFilePath); err != nil {
		return fmt.Errorf("failed to delete JSONL file %s: %w", jsonFilePath, err)
	}
	activeDuration := time.Since(startTime)
	slog.Debug("converted JSONL to Parquet", "file", jsonFilePath, "duration (ms)", activeDuration.Milliseconds())
	job.UpdateActiveDuration(activeDuration)
	return nil
}

// convert the given jsonl file to parquet
func (w *parquetConversionWorker) convertFile(jsonlFilePath string, partition *config.Partition, schema *schema.RowSchema) (int64, error) {
	//slog.Debug("worker.convertFile", "jsonlFilePath", jsonlFilePath, "partitionName", partitionName)
	//defer slog.Debug("worker.convertFile - done", "error", err)

	// verify the jsonl file has a .jsonl extension
	if filepath.Ext(jsonlFilePath) != ".jsonl" {
		return 0, fmt.Errorf("invalid file type - parquetConversionWorker only supports JSONL files: %s", jsonlFilePath)
	}
	// verify file exists
	if _, err := os.Stat(jsonlFilePath); os.IsNotExist(err) {
		return 0, fmt.Errorf("file does not exist: %s", jsonlFilePath)
	}

	// determine the root based on the partition

	if err := os.MkdirAll(filepath.Dir(w.destDir), 0755); err != nil {
		return 0, fmt.Errorf("failed to create parquet folder: %w", err)
	}

	// get the query format - from cache if possible
	selectQueryFormat := w.getViewQuery(partition.UnqualifiedName, schema)
	// render query
	selectQuery := fmt.Sprintf(selectQueryFormat.(string), jsonlFilePath)

	// if the partition includes a filter, add a WHERE clause
	// TODO add validation
	if partition.Filter != "" {
		selectQuery += fmt.Sprintf(" WHERE %s", partition.Filter)
	}
	// Create a query to write to partitioned parquet files

	// TODO review to ensure we are safe from SQL injection
	// https://github.com/turbot/tailpipe/issues/67

	// get a unique file root
	fileRoot := w.fileRootProvider.GetFileRoot()

	partitionColumns := []string{constants.TpTable, constants.TpPartition, constants.TpIndex, constants.TpDate}
	exportQuery := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET, PARTITION_BY (%s), OVERWRITE_OR_IGNORE, FILENAME_PATTERN "%s_{i}");`,
		selectQuery, w.destDir, strings.Join(partitionColumns, ","), fileRoot)

	_, err := w.db.Exec(exportQuery)
	if err != nil {
		return 0, fmt.Errorf("failed to export data to parquet: %w", err)
	}

	// now read row count
	count, err := getRowCount(w.db.DB, w.destDir, fileRoot, partition.TableName)
	if err != nil {
		slog.Warn("failed to get row count - conversion failed", "error", err, "query", exportQuery)
	}
	return count, err

}

func getRowCount(db *sql.DB, destDir, fileRoot, table string) (int64, error) {
	// Build the query
	rowCountQuery := fmt.Sprintf(`SELECT SUM(num_rows) FROM parquet_file_metadata('%s')`, filepaths.GetParquetFileGlobForTable(destDir, table, fileRoot)) //nolint:gosec // fixed sql query

	// Execute the query and scan the result directly
	var rowCount int64
	err := db.QueryRow(rowCountQuery).Scan(&rowCount)
	if err != nil {
		// if this is an IO error caused by no files being found, return 0
		if strings.Contains(err.Error(), "No files found") {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to query row count: %w", err)
	}

	return rowCount, nil
}

func (w *parquetConversionWorker) getViewQuery(partitionName string, rowSchema *schema.RowSchema) interface{} {
	query, ok := w.viewQueries[partitionName]
	if !ok {
		query = buildViewQuery(rowSchema)
		w.viewQueries[partitionName] = query
	}
	return query
}

func buildViewQuery(rowSchema *schema.RowSchema) string {
	var structSliceColumns []*schema.ColumnSchema

	// first build the columns to select from the jsonl file
	var columnStrings strings.Builder
	for i, column := range rowSchema.Columns {
		if i > 0 {
			columnStrings.WriteString(",\n")
		}

		columnStrings.WriteString(getSqlForField(column, 1))
		if column.Type == "STRUCT[]" {
			structSliceColumns = append(structSliceColumns, column)
		}
		// TODO take nested struct arays into account
		//for _, nestedColumn := range column.StructFields {
		//	if nestedColumn.Type == "STRUCT[]" {
		//		structSliceColumns = append(structSliceColumns, nestedColumn)
		//	}
		//
		//}
	}

	// build column definitions
	columnDefinitions := getReadJSONColumnDefinitions(rowSchema)

	columnStrings.WriteString(fmt.Sprintf(`
FROM
	read_ndjson(
		'%%s',
%s
	)`, helpers.Tabify(columnDefinitions, "\t\t")))

	// if there are no struct[] fields, we are done - just add the select at the start
	if len(structSliceColumns) == 0 {
		return fmt.Sprintf("SELECT\n%s", columnStrings.String())
	}

	// if there are struct[] fields, we need to build a more complex query

	// add row number in case of potential grouping
	var str strings.Builder
	str.WriteString("SELECT\n")
	str.WriteString("\trow_number() OVER () AS rowid,\n")
	str.WriteString(columnStrings.String())

	// build the struct slice query
	return getViewQueryForStructSlices(str.String(), rowSchema, structSliceColumns)

}

// return the column definitions for the row schema, in the format required for the duck db read_json_auto function
func getReadJSONColumnDefinitions(rowSchema *schema.RowSchema) string {
	// columns = {BooleanField: 'BOOLEAN', BooleanField2: 'BOOLEAN', BooleanField3: 'BOOLEAN'})
	var str strings.Builder
	str.WriteString("columns = {")
	for i, column := range rowSchema.Columns {
		if i > 0 {
			str.WriteString(", ")
		}
		str.WriteString(fmt.Sprintf(`
	"%s": '%s'`, column.SourceName, column.FullType()))
	}
	str.WriteString("\n}")
	return str.String()
}

func getViewQueryForStructSlices(q string, rowSchema *schema.RowSchema, structSliceColumns []*schema.ColumnSchema) string {
	var str strings.Builder

	/* this is the what we want

	WITH raw AS (
	    SELECT
	        row_number() OVER () AS rowid,
	        "StructArrayField" AS "struct_array_field",
	        "IntField" AS "int_field",
	        "StringField" AS "string_field",
	        "FloatField" AS "float_field",
	        "BooleanField" AS "boolean_field",
	        "IntArrayField" AS "int_array_field",
	        "StringArrayArrayField" AS "string_array_field",
	        "FloatArrayField" AS "float_array_field",
	        "BooleanArrayField" AS "boolean_array_field"
	    FROM
	        read_ndjson(
	            '/Users/kai/Dev/github/turbot/tailpipe/internal/parquet/buildViewQuery_test_data/1.jsonl',
	            columns = {
	                "StructArrayField": 'STRUCT("StructStringField" VARCHAR, "StructIntField" INTEGER)[]',
	                "IntField": 'INTEGER',
	                "StringField": 'VARCHAR',
	                "FloatField": 'FLOAT',
	                "BooleanField": 'BOOLEAN',
	                "IntArrayField": 'INTEGER[]',
	                "StringArrayArrayField": 'VARCHAR[]',
	                "FloatArrayField": 'FLOAT[]',
	                "BooleanArrayField": 'BOOLEAN[]'
	            }
	        )
	), unnest_struct_array_field AS (
	    SELECT
	        rowid,
	        UNNEST(COALESCE("struct_array_field", ARRAY[]::STRUCT("StructStringField" VARCHAR, "StructIntField" INTEGER)[])::STRUCT("StructStringField" VARCHAR, "StructIntField" INTEGER)[]) AS struct_array_field
	    FROM
	        raw
	), rebuild_unnest_struct_array_field AS (
	    SELECT
	        rowid,
	        struct_array_field->>'StructStringField' AS StructArrayField_StructStringField,
	        struct_array_field->>'StructIntField' AS StructArrayField_StructIntField
	    FROM
	        unnest_struct_array_field
	), grouped_unnest_struct_array_field AS (
	    SELECT
	        rowid,
	        array_agg(struct_pack(
	            struct_string_field := StructArrayField_StructStringField::VARCHAR,
	            struct_int_field := StructArrayField_StructIntField::INTEGER
	        )) AS struct_array_field
	    FROM
	        rebuild_unnest_struct_array_field
	    GROUP BY
	        rowid
	)
	SELECT
	    COALESCE(joined_struct_array_field.struct_array_field, NULL) AS struct_array_field,
	    raw.int_field,
	    raw.string_field,
	    raw.float_field,
	    raw.boolean_field,
	    raw.int_array_field,
	    raw.string_array_field,
	    raw.float_array_field,
	    raw.boolean_array_field
	FROM
	    raw
	LEFT JOIN
	    grouped_unnest_struct_array_field joined_struct_array_field ON raw.rowid = joined_struct_array_field.rowid;
	*/

	/* 	WITH raw AS (

	 */
	str.WriteString("WITH raw AS (\n")
	str.WriteString(helpers.Tabify(q, "\t"))
	str.WriteString("\n)")

	// for every struct[] field, we need to add various CTEs
	for _, structSliceCol := range structSliceColumns {

		/*
			, unnest_struct_array_field AS (
			    SELECT
			        rowid,
		*/
		unnestName := fmt.Sprintf(`unnest_%s`, structSliceCol.ColumnName)

		str.WriteString(fmt.Sprintf(`, %s AS (
    SELECT
        rowid,`, unnestName))

		/*
			UNNEST(COALESCE("struct_array_field", ARRAY[]::STRUCT("StructStringField" VARCHAR, "StructIntField" INTEGER)[])::STRUCT("StructStringField" VARCHAR, "StructIntField" INTEGER)[]) AS struct_array_field
		*/

		str.WriteString(fmt.Sprintf(`
		UNNEST(COALESCE("%s", ARRAY[]::%s)::%s) AS %s`, structSliceCol.ColumnName, structSliceCol.FullType(), structSliceCol.FullType(), structSliceCol.ColumnName))

		/*
		   	FROM
		   		raw
		   )
		*/

		str.WriteString(`
	FROM
		raw
)`)

		/*
		   , rebuild_unnest_struct_array_field AS (
		      SELECT
		   	   rowid,
		*/
		rebuildName := fmt.Sprintf(`rebuild_%s`, unnestName)

		str.WriteString(fmt.Sprintf(`, %s AS (
	SELECT
		rowid`, rebuildName))

		// loop over fields in the struct
		/*
			        struct_array_field->>'StructStringField' AS StructArrayField_StructStringField,
			)
		*/
		for _, structField := range structSliceCol.StructFields {
			str.WriteString(",\n")
			str.WriteString(fmt.Sprintf(`		%s->>'%s' AS %s_%s`, structSliceCol.ColumnName, structField.SourceName, structSliceCol.SourceName, structField.SourceName))
		}
		/*
		   	FROM
		   		unnest_struct_array_field
		   )
		*/

		str.WriteString(fmt.Sprintf(`
	FROM
		%s
)`, unnestName))

		/*
		      , grouped_unnest_struct_array_field AS (
		      	    SELECT
		      	        rowid,
		      	        array_agg(struct_pack(
		   				struct_string_field := StructArrayField_StructStringField::VARCHAR,
		      	            struct_int_field := StructArrayField_StructIntField::INTEGER
		      	        )) AS struct_array_field
		      	    FROM
		      	        rebuild_unnest_struct_array_field
		      	    GROUP BY
		      	        rowid
		      	)

		*/
		groupedName := fmt.Sprintf(`grouped_%s`, unnestName)
		str.WriteString(fmt.Sprintf(`, %s AS (`, groupedName))
		str.WriteString(`
	SELECT
		rowid,	
		array_agg(struct_pack(
`)
		// loop over struct
		for i, structField := range structSliceCol.StructFields {
			if i > 0 {
				str.WriteString(",\n")
			}
			str.WriteString(fmt.Sprintf(`				%s := %s_%s::%s`, structField.ColumnName, structSliceCol.SourceName, structField.SourceName, structField.Type))
		}
		str.WriteString(fmt.Sprintf(`
		)) AS %s	
	FROM
		%s	
	GROUP BY
		rowid	
)`, structSliceCol.ColumnName, rebuildName))

	}
	// build the final select
	/*
		SELECT
			    COALESCE(joined_struct_array_field.struct_array_field, NULL) AS struct_array_field,
			    raw.int_field,
			    raw.string_field,
			    raw.float_field,
			    raw.boolean_field,
			    raw.int_array_field,
			    raw.string_array_field,
			    raw.float_array_field,
			    raw.boolean_array_field
			FROM
			    raw
			LEFT JOIN
			    grouped_unnest_struct_array_field joined_struct_array_field ON raw.rowid = joined_struct_array_field.rowid;
	*/
	// build list of coalesce fields and join fields
	var coalesceFields strings.Builder
	var leftJoins strings.Builder
	for i, column := range structSliceColumns {
		if i > 0 {
			coalesceFields.WriteString(",\n")
			leftJoins.WriteString("\n")
		}
		joinedName := fmt.Sprintf(`joined_%s`, column.ColumnName)
		groupedName := fmt.Sprintf(`grouped_unnest_%s`, column.ColumnName)

		coalesceFields.WriteString(fmt.Sprintf(`	COALESCE(%s.%s, NULL) AS %s`, joinedName, column.ColumnName, column.ColumnName))
		leftJoins.WriteString(fmt.Sprintf(`LEFT JOIN
	%s %s ON raw.rowid = %s.rowid`, groupedName, joinedName, joinedName))

	}

	// now construct final select
	str.WriteString(fmt.Sprintf(`
SELECT
%s`, coalesceFields.String()))

	for _, column := range rowSchema.Columns {
		if column.Type != "STRUCT[]" {
			str.WriteString(",\n")
			str.WriteString(fmt.Sprintf(`	raw.%s`, column.ColumnName))
		}
	}
	str.WriteString(fmt.Sprintf(`
FROM
	raw	
%s`, leftJoins.String()))

	return str.String()
}

// Return the SQL line to select the given field
func getSqlForField(column *schema.ColumnSchema, tabs int) string {
	// Calculate the tab spacing
	tab := strings.Repeat("\t", tabs)

	switch column.Type {
	case "STRUCT":
		var str strings.Builder

		// Start CASE logic to handle NULL values for the struct
		str.WriteString(fmt.Sprintf("%sCASE\n", tab))
		str.WriteString(fmt.Sprintf("%s\tWHEN \"%s\" IS NULL THEN NULL\n", tab, column.SourceName))
		str.WriteString(fmt.Sprintf("%s\tELSE struct_pack(\n", tab))

		// Add nested fields to the struct_pack
		for j, nestedColumn := range column.StructFields {
			if j > 0 {
				str.WriteString(",\n")
			}
			parentName := fmt.Sprintf("\"%s\"", column.SourceName)
			str.WriteString(getTypeSqlForStructField(nestedColumn, parentName, tabs+2))
		}

		// Close struct_pack and CASE
		str.WriteString(fmt.Sprintf("\n%s\t)\n", tab))
		str.WriteString(fmt.Sprintf("%sEND AS \"%s\"", tab, column.ColumnName))
		return str.String()

	case "JSON":
		// Convert the value using json()
		return fmt.Sprintf("%sjson(\"%s\") AS \"%s\"", tab, column.SourceName, column.ColumnName)

	default:
		// Scalar fields
		return fmt.Sprintf("%s\"%s\" AS \"%s\"", tab, column.SourceName, column.ColumnName)
	}
}

// Return the SQL line to pack the given field as a struct
func getTypeSqlForStructField(column *schema.ColumnSchema, parentName string, tabs int) string {
	tab := strings.Repeat("\t", tabs)

	switch column.Type {
	case "STRUCT":
		var str strings.Builder

		// Add CASE logic to handle NULL values for the struct
		str.WriteString(fmt.Sprintf("%s\"%s\" := CASE\n", tab, column.ColumnName))
		str.WriteString(fmt.Sprintf("%s\tWHEN %s.\"%s\" IS NULL THEN NULL\n", tab, parentName, column.SourceName))
		str.WriteString(fmt.Sprintf("%s\tELSE struct_pack(\n", tab))

		// Loop through nested fields and add them to the struct_pack
		for j, nestedColumn := range column.StructFields {
			if j > 0 {
				str.WriteString(",\n")
			}
			// Use the current field as the new parent for recursion
			newParent := fmt.Sprintf("%s.\"%s\"", parentName, column.SourceName)
			str.WriteString(getTypeSqlForStructField(nestedColumn, newParent, tabs+2))
		}

		// Close struct_pack and CASE
		str.WriteString(fmt.Sprintf("\n%s\t)\n", tab))
		str.WriteString(fmt.Sprintf("%sEND", tab))
		return str.String()

	default:
		// Scalar fields
		return fmt.Sprintf("%s\"%s\" := %s.\"%s\"::%s", tab, column.ColumnName, parentName, column.SourceName, column.Type)
	}
}
