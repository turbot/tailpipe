package parquet

import (
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/plugin"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

/*
https://duckdb.org/docs/extensions/json.html
If multiple values need to be extracted from the same JSON, it is more efficient to extract a list of paths:

The following will cause the JSON to be parsed twice,:

Resulting in a slower query that uses more memory:

SELECT
    json_extract(j, 'family') AS family,
    json_extract(j, 'species') AS species
FROM example;

family	species
"anatidae"	["duck","goose","swan",null]
The following produces the same result but is faster and more memory-efficient:

WITH extracted AS (
    SELECT json_extract(j, ['family', 'species']) AS extracted_list
    FROM example
)
SELECT
    extracted_list[1] AS family,
    extracted_list[2] AS species
*/

// parquetConversionWorker is an implementation of worker that converts JSONL files to Parquet
type parquetConversionWorker struct {
	fileWorkerBase[ParquetJobPayload]
	db *duckDb
}

// ctor
func newParquetConversionWorker(jobChan chan fileJob[ParquetJobPayload], errorChan chan jobGroupError, sourceDir, destDir string) (worker, error) {
	w := &parquetConversionWorker{
		fileWorkerBase: newWorker(jobChan, errorChan, sourceDir, destDir),
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

func (w *parquetConversionWorker) doJSONToParquetConversion(job fileJob[ParquetJobPayload]) error {
	// build the source filename
	jsonFileName := plugin.ExecutionIdToFileName(job.groupId, job.chunkNumber)
	jsonFilePath := filepath.Join(w.sourceDir, jsonFileName)

	// process the jobGroup
	err := w.convertFile(jsonFilePath, job.collectionType, job.payload.Schema)
	if err != nil {
		slog.Error("failed to convert file", "error", err)
		return fmt.Errorf("failed to convert file %s: %w", jsonFilePath, err)
	}

	// delete JSON file (configurable?)
	if err := os.Remove(jsonFilePath); err != nil {
		return fmt.Errorf("failed to delete JSONL file %s: %w", jsonFilePath, err)
	}
	return nil
}

// convert the given jsonl file to parquet
func (w *parquetConversionWorker) convertFile(jsonlFilePath, collectionType string, schema *schema.RowSchema) (err error) {
	//slog.Debug("worker.convertFile", "jsonlFilePath", jsonlFilePath, "collectionType", collectionType)
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

	// determine the root based on the collection type
	fileRoot := w.getParquetFileRoot(collectionType)
	if err := os.MkdirAll(filepath.Dir(fileRoot), 0755); err != nil {
		return fmt.Errorf("failed to create parquet folder: %w", err)
	}

	selectQuery := buildViewQuery(schema, jsonlFilePath)
	// Create a query to write to partitioned parquet files
	partitionColumns := []string{"tp_collection", "tp_connection", "tp_year", "tp_month", "tp_day"}
	exportQuery := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET, PARTITION_BY (%s), OVERWRITE_OR_IGNORE, FILENAME_PATTERN "file_{uuid}");`, selectQuery, fileRoot, strings.Join(partitionColumns, ","))

	_, err = w.db.Exec(exportQuery)
	if err != nil {
		return fmt.Errorf("failed to export data to parquet: %w", err)
	}

	//slog.Debug("exported data to parquet", "file", filePath)

	return nil
}

// getParquetFileRoot generates the file root for the parquet file based on the naming convention
func (w *parquetConversionWorker) getParquetFileRoot(collectionType string) string {
	return filepath.Join(w.destDir, collectionType)
}

func buildViewQuery(rowSchema *schema.RowSchema, jsonlFilePath string) string {
	var structSliceColumns []*schema.ColumnSchema

	/* this is out goal:

	-- scalar data

	WITH json_data AS (
	    SELECT
	        json_extract(json, ['BooleanField', 'TinyIntField', 'SmallIntField', 'IntegerField', 'BigIntField', 'UTinyIntField', 'USmallIntField', 'UIntegerField', 'UBigIntField', 'FloatField', 'DoubleField', 'VarcharField', 'TimestampField']) AS extracted_list
	    FROM
	        read_json_auto('/Users/kai/Dev/github/turbot/tailpipe/internal/parquet/buildViewQuery_test_data/test.jsonl', format='newline_delimited')
	)
	  SELECT
	      COALESCE(CAST(extracted_list[1] AS BOOLEAN), NULL) AS boolean_field,
	      COALESCE(CAST(extracted_list[2] AS TINYINT), NULL) AS tinyint_field,
	      COALESCE(CAST(extracted_list[3] AS SMALLINT), NULL) AS smallint_field,
	      COALESCE(CAST(extracted_list[4] AS INTEGER), NULL) AS integer_field,
	      COALESCE(CAST(extracted_list[5] AS BIGINT), NULL) AS bigint_field,
	      COALESCE(CAST(extracted_list[6] AS TINYINT), NULL) AS utinyint_field,
	      COALESCE(CAST(extracted_list[7] AS SMALLINT), NULL) AS usmallint_field,
	      COALESCE(CAST(extracted_list[8] AS INTEGER), NULL) AS uinteger_field,
	      COALESCE(CAST(extracted_list[9] AS BIGINT), NULL) AS ubigint_field,
	      COALESCE(CAST(extracted_list[10] AS FLOAT), NULL) AS float_field,
	      COALESCE(CAST(extracted_list[11] AS DOUBLE), NULL) AS double_field,
	      COALESCE(CAST(extracted_list[12] AS VARCHAR), NULL) AS varchar_field,
	      COALESCE(CAST(extracted_list[13] AS TIMESTAMP), NULL) AS timestamp_field
	  FROM
	      json_data;

	*/

	// first build the columns to select from the jsonl file
	var columnStrings strings.Builder

	columnStrings.WriteString(fmt.Sprintf(`WITH json_data AS (
	SELECT
		json_extract(json, ['%s']) AS extracted_list
	FROM
		read_json_auto('%s', format='newline_delimited')
),
parsed_data AS (
	SELECT
`, strings.Join(rowSchema.ColumnSourceNames(), "', '"), jsonlFilePath))

	for i, column := range rowSchema.Columns {
		if i > 0 {
			columnStrings.WriteString(",\n")
		}
		columnStrings.WriteString(fmt.Sprintf("%s", getSqlForField(column, 2, i+1)))
		if column.Type == "STRUCT[]" {
			structSliceColumns = append(structSliceColumns, column)
		}
	}
	columnStrings.WriteString(fmt.Sprintf(`	
	FROM
		json_data
)`))

	// if there are no struct[] fields, we are done - just add the select at the start
	if len(structSliceColumns) == 0 {
		columnStrings.WriteString(`
SELECT
	* 
FROM
	parsed_data`)
		return columnStrings.String()
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

func getViewQueryForStructSlices(q string, rowSchema *schema.RowSchema, structSliceColumns []*schema.ColumnSchema) string {
	var str strings.Builder

	/* this is the what we want
	WITH sl AS (
		SELECT
			row_number() OVER () AS rowid,
			UNNEST(StructArrayField) AS StructArrayField,
			IntField::INTEGER AS int_field,
		FROM
			read_json_auto('/Users/kai/Dev/github/turbot/tailpipe/internal/parquet/buildViewQuery_test_data/test.jsonl', format='newline_delimited')
	), json_data AS (
		SELECT
			rowid,
			StructArrayField->>'StructStringField' AS StructArrayField_StructStringField,
			StructArrayField->>'StructIntField' AS StructArrayField_StructIntField
		FROM
			sl
	)
	SELECT
		array_agg(struct_pack(
			struct_string_field := StructArrayField_StructStringField::VARCHAR,
			struct_int_field := StructArrayField_StructIntField::INTEGER
		)) AS struct_array_field,
		sl.int_field
	FROM
		json_data
	JOIN
		sl ON json_data.rowid = sl.rowid
	GROUP BY
		sl.rowid, sl.int_field

	*/

	/* 	WITH sl AS (
	SELECT
		row_number() OVER () AS rowid,
		UNNEST(StructArrayField) AS StructArrayField,
		IntField::INTEGER AS int_field,
	FROM
		read_json_auto('/Users/kai/Dev/github/turbot/tailpipe/internal/parquet/buildViewQuery_test_data/test.jsonl', format='newline_delimited')

	*/
	str.WriteString("WITH sl AS (\n")
	str.WriteString(helpers.Tabify(q, "\t"))

	/*
		), json_data AS (
					SELECT
						rowid,
	*/

	str.WriteString("\n), json_data AS (\n")
	str.WriteString("\tSELECT\n")
	str.WriteString("\t\trowid")

	/*
				  StructArrayField->>'StructStringField' AS StructArrayField_StructStringField,
				  StructArrayField->>'StructIntField' AS StructArrayField_StructIntField
			  FROM sl
		)
	*/
	for _, structSliceCol := range structSliceColumns {
		// iterate over struct fields
		for _, col := range structSliceCol.StructFields {

			str.WriteString(",\n")
			// json_extract(<ParentStructSourceName>, '$.<SourceName>') AS <ParentStructSourceName>_<SourceName>
			str.WriteString(fmt.Sprintf("\t\t%s->>'%s' AS %s_%s", structSliceCol.SourceName, col.SourceName, structSliceCol.SourceName, col.SourceName))
		}
	}
	str.WriteString("\n\tFROM\n\t\tsl\n)")

	/*

		)
		SELECT
			array_agg(struct_pack(
				struct_string_field := StructArrayField_StructStringField::VARCHAR,
				struct_int_field := StructArrayField_StructIntField::INTEGER
			)) AS struct_array_field,
			sl.int_field
	*/

	str.WriteString("\nSELECT\n")

	for i, column := range rowSchema.Columns {
		if i > 0 {
			str.WriteString(",\n")
		}
		if column.Type == "STRUCT[]" {
			/*
				array_agg(struct_pack(
					struct_string_field := json_data.StructArrayField_StructStringField,
					struct_int_field := json_data.StructArrayField_StructIntField
				)) AS struct_array_field,
			*/
			str.WriteString(fmt.Sprintf("\tarray_agg(struct_pack(\n"))
			for j, nestedColumn := range column.StructFields {
				if j > 0 {
					str.WriteString(",\n")
				}
				// <ColumnName> := <StructFieldSourceName>_<StructFieldSourceName>::TYPE,
				str.WriteString(fmt.Sprintf("\t\t%s := %s_%s::%s", nestedColumn.ColumnName, column.SourceName, nestedColumn.SourceName, nestedColumn.Type))
			}
			str.WriteString(fmt.Sprintf("\n\t)) AS %s", column.ColumnName))
		} else {

			str.WriteString(fmt.Sprintf("\tsl.%s", column.ColumnName))
		}
	}
	/*
		FROM
			json_data
		JOIN
			sl ON json_data.rowid = sl.rowid
		GROUP BY
			sl.rowid, sl.int_field
	*/
	str.WriteString("\nFROM\n\tjson_data")
	str.WriteString("\nJOIN\n\tsl ON json_data.rowid = sl.rowid")
	str.WriteString("\nGROUP BY\n\tsl.rowid")
	for _, column := range rowSchema.Columns {
		if column.Type != "STRUCT[]" {
			str.WriteString(fmt.Sprintf(", sl.%s", column.ColumnName))
		}
	}
	return str.String()
}

// return the sql line to select the given field
func getSqlForField(column *schema.ColumnSchema, tabs int, fieldNum int) string {

	/* for scalar fields this wll be
	    <SourceName>::<Type> AS <ColumnName>
	e.g.
		tpIps::VARCHAR[] AS tp_ips,


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

	case "MAP":
		//	// TODO
		return fmt.Sprintf("%s%s AS %s", tab, column.SourceName, column.ColumnName)

	case "STRUCT[]":
		// //UNNEST(COALESCE(StructArrayField, ARRAY[]::STRUCT(StructStringField VARCHAR, StructIntField INTEGER)[])::STRUCT(StructStringField VARCHAR, StructIntField INTEGER)[]) AS StructArrayField
		// build DuckDB struct typedef
		//STRUCT(StructStringField VARCHAR, StructIntField INTEGER)[]
		structTypeDef := column.FullType()

		// just unnest - we will add a clause to extract the fields
		return fmt.Sprintf("%sUNNEST(COALESCE(%s, ARRAY[]::%s)::%s) AS %s", tab, column.SourceName, structTypeDef, structTypeDef, column.SourceName)

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
			str.WriteString(fmt.Sprintf("%s", getTypeSqlForStructField(nestedColumn, column.SourceName, tabs+1)))
		}
		str.WriteString(fmt.Sprintf("\n%s) AS %s", tab, column.ColumnName))
		return str.String()
	default:
		//			 COALESCE(CAST(extracted_list[<fieldnum>] AS <TYPE>), NULL) AS <ColumnName>,
		return fmt.Sprintf("%sCOALESCE(CAST(extracted_list[%d] AS %s), NULL) AS %s", tab, fieldNum, column.Type, column.ColumnName)
	}

}

// return the sql line to pack the given field as a struct
// this will use StructFields to build a struct_pack command
func getTypeSqlForStructField(column *schema.ColumnSchema, parentName string, tabs int) string {
	tab := strings.Repeat("\t", tabs)
	switch column.Type {
	//case "MAP":
	//	// TODO

	case "STRUCT":
		var str strings.Builder
		str.WriteString(fmt.Sprintf("%s%s := struct_pack(\n", tab, column.ColumnName))
		for j, nestedColumn := range column.StructFields {
			if j > 0 {
				str.WriteString(",\n")
			}
			// newParent is the parentName for the nested column
			newParent := fmt.Sprintf("%s.%s", parentName, column.SourceName)
			str.WriteString(fmt.Sprintf("%s", getTypeSqlForStructField(nestedColumn, newParent, tabs+1)))
		}
		str.WriteString(fmt.Sprintf("\n%s)", tab))
		return str.String()
	default:
		return fmt.Sprintf("%s%s := %s.%s::%s", tab, column.ColumnName, parentName, column.SourceName, column.Type)
	}
}
