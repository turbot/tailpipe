package parquet

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/plugin"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// parquetConversionWorker is an implementation of worker that converts JSONL files to Parquet
type parquetConversionWorker struct {
	fileWorkerBase[JobPayload]
	// cache partition schemas - key by partition name
	viewQueries map[string]string

	db *duckDb
}

// ctor
func newParquetConversionWorker(jobChan chan fileJob[JobPayload], errorChan chan jobGroupError, sourceDir, destDir string) (worker, error) {
	w := &parquetConversionWorker{
		fileWorkerBase: newWorker(jobChan, errorChan, sourceDir, destDir),
		viewQueries:    make(map[string]string),
	}

	// create a new DuckDB instance
	db, err := newDuckDb()
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB wrapper: %w", err)
	}
	w.db = db
	// set base doWork function
	w.doWorkFunc = w.doJSONToParquetConversion
	w.closeFunc = w.close
	return w, nil

}

func (w *parquetConversionWorker) close() {
	w.db.Close()
}

func (w *parquetConversionWorker) doJSONToParquetConversion(job fileJob[JobPayload]) error {
	startTime := time.Now()

	// build the source filename
	jsonFileName := plugin.ExecutionIdToFileName(job.groupId, job.chunkNumber)
	jsonFilePath := filepath.Join(w.sourceDir, jsonFileName)

	// process the jobGroup
	err := w.convertFile(jsonFilePath, job.payload.PartitionName, job.payload.Schema)
	if err != nil {
		slog.Error("failed to convert file", "error", err)
		return fmt.Errorf("failed to convert file %s: %w", jsonFilePath, err)
	}

	// delete JSON file (configurable?)
	if err := os.Remove(jsonFilePath); err != nil {
		return fmt.Errorf("failed to delete JSONL file %s: %w", jsonFilePath, err)
	}
	activeDuration := time.Since(startTime)
	slog.Debug("converted JSONL to Parquet", "file", jsonFilePath, "duration (ms)", activeDuration.Milliseconds())
	job.payload.UpdateActiveDuration(activeDuration)
	return nil
}

// convert the given jsonl file to parquet
func (w *parquetConversionWorker) convertFile(jsonlFilePath, partitionName string, schema *schema.RowSchema) (err error) {
	//slog.Debug("worker.convertFile", "jsonlFilePath", jsonlFilePath, "partitionName", partitionName)
	//defer slog.Debug("worker.convertFile - done", "error", err)

	// TODO should this also handle CSV - configure the worker?
	// verify the jsonl file has a .jsonl extension
	if filepath.Ext(jsonlFilePath) != ".jsonl" {
		return fmt.Errorf("invalid file type - parquetConversionWorker only supports JSONL files: %s", jsonlFilePath)
	}
	// verify file exists
	if _, err := os.Stat(jsonlFilePath); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", jsonlFilePath)
	}

	// determine the root based on the partition
	fileRoot := w.getParquetFileRoot(partitionName)
	if err := os.MkdirAll(filepath.Dir(fileRoot), 0755); err != nil {
		return fmt.Errorf("failed to create parquet folder: %w", err)
	}

	// get the query format - from cache if possible
	selectQueryFormat := w.getViewQuery(partitionName, schema)
	// render query
	selectQuery := fmt.Sprintf(selectQueryFormat.(string), jsonlFilePath)

	// Create a query to write to partitioned parquet files
	partitionColumns := []string{"tp_partition", "tp_index", "tp_year", "tp_month", "tp_day"}
	exportQuery := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET, PARTITION_BY (%s), OVERWRITE_OR_IGNORE, FILENAME_PATTERN "file_{uuid}");`, selectQuery, fileRoot, strings.Join(partitionColumns, ","))

	_, err = w.db.Exec(exportQuery)
	if err != nil {
		return fmt.Errorf("failed to export data to parquet: %w", err)
	}

	//slog.Debug("exported data to parquet", "file", filePath)

	return nil
}

// getParquetFileRoot generates the file root for the parquet file based on the naming convention
func (w *parquetConversionWorker) getParquetFileRoot(partitionName string) string {
	// TODO #parquet should this be partition_type/partition_name ot what?
	return filepath.Join(w.destDir, partitionName)
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
	}

	// build column defintions
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

// return the sql line to select the given field
func getSqlForField(column *schema.ColumnSchema, tabs int) string {

	/* for scalar fields this wll be
	    <SourceName> AS <ColumnName>
	e.g.
		tpIps AS tp_ips,

		for struct fields this will be

		struct_pack(
			<ColumnName> := <ParentSourceName>.<SourceName>::<Type>,

		where ParentSourceName is the SourceName of the struct field
	e.g.
		type_field := userIdentity.nested.typeField::VARCHAR,

	struct_pack(
		        type_feld := userIdentity.typeField::VARCHAR,
		        session_context := userIdentity.sessionContext::VARCHAR
				)
	*/

	// calculate the tab
	tab := strings.Repeat("\t", tabs)
	switch column.Type {

	// TODO #parquet https://github.com/turbot/tailpipe/issues/new
	//case "MAP":
	//
	//	return fmt.Sprintf(`%s"%s" AS "%s"`, tab, column.SourceName, column.ColumnName)

	case "STRUCT":
		//  struct_pack(
		//  <StructColumnName> := <ParentSourceName>.<SourceName>::<Type>,
		//  ...) AS <ColumnName>

		var str strings.Builder
		str.WriteString(fmt.Sprintf("%sstruct_pack(\n", tab))
		for j, nestedColumn := range column.StructFields {
			if j > 0 {
				str.WriteString(",\n")
			}
			parentName := fmt.Sprintf(`"%s"`, column.SourceName)
			str.WriteString(getTypeSqlForStructField(nestedColumn, parentName, tabs+1))
		}
		str.WriteString(fmt.Sprintf(`
%s) AS "%s"`, tab, column.ColumnName))
		return str.String()
	default:
		//<SourceName> AS <ColumnName>
		return fmt.Sprintf(`%s"%s" AS "%s"`, tab, column.SourceName, column.ColumnName)
	}

}

// return the sql line to pack the given field as a struct
// this will use StructFields to build a struct_pack command
func getTypeSqlForStructField(column *schema.ColumnSchema, parentName string, tabs int) string {
	tab := strings.Repeat("\t", tabs)
	switch column.Type {
	//case "MAP":
	// TODO #parquet https://github.com/turbot/tailpipe/issues/new

	case "STRUCT":
		var str strings.Builder
		str.WriteString(fmt.Sprintf(`%s"%s" := struct_pack(`, tab, column.ColumnName))
		str.WriteString("\n")
		for j, nestedColumn := range column.StructFields {
			if j > 0 {
				str.WriteString(",\n")
			}
			// newParent is the parentName for the nested column
			newParent := fmt.Sprintf(`%s."%s"`, parentName, column.SourceName)
			str.WriteString(getTypeSqlForStructField(nestedColumn, newParent, tabs+1))
		}
		str.WriteString(fmt.Sprintf("\n%s)", tab))
		return str.String()
	default:
		return fmt.Sprintf(`%s"%s" := %s."%s"::%s`, tab, column.ColumnName, parentName, column.SourceName, column.Type)
	}
}
