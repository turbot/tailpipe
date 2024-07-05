package parquet

import (
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/turbot/tailpipe-plugin-sdk/plugin"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

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

	// first build the columns to select from the jsonl file
	var columnStrings strings.Builder
	for i, column := range rowSchema.Columns {
		if i > 0 {
			columnStrings.WriteString(",\n")
		}
		columnStrings.WriteString(fmt.Sprintf("%s", getSqlForField(column, 1)))
		if column.Type == "STRUCT[]" {
			structSliceColumns = append(structSliceColumns, column)
		}
	}
	columnStrings.WriteString(fmt.Sprintf("\nFROM\n\tread_json_auto('%s', format='newline_delimited')", jsonlFilePath))

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
		for _, col := range structSliceCol.ChildFields {

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
		if column.Type == "STRUCT[]" {
			/*
				array_agg(struct_pack(
					struct_string_field := json_data.StructArrayField_StructStringField,
					struct_int_field := json_data.StructArrayField_StructIntField
				)) AS struct_array_field,
			*/
			str.WriteString(fmt.Sprintf("\tarray_agg(struct_pack(\n"))
			for j, nestedColumn := range column.ChildFields {
				if i > 0 || j > 0 {
					str.WriteString(",\n")
				}
				// <ColumnName> := <StructFieldSourceName>_<StructFieldSourceName>::TYPE,
				str.WriteString(fmt.Sprintf("\t\t%s := %s_%s::%s", nestedColumn.ColumnName, column.SourceName, nestedColumn.SourceName, nestedColumn.Type))
			}
			str.WriteString(fmt.Sprintf("\n\t)) AS %s", column.ColumnName))
		} else {
			if i > 0 {
				str.WriteString(",\n")
			}
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
func getSqlForField(column *schema.ColumnSchema, tabs int) string {

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

		/*
			--ATTEMPTS
													THIS WORKS but need to read from JSON
													-- Create the table
													CREATE TABLE json_data (
													    StructStringField VARCHAR,
													    StructIntField INTEGER
													);

													-- Insert data
													INSERT INTO json_data VALUES
													('StringValue1', 1),
													('StringValue2', 2);

													-- Use list and struct_pack to construct a list of structs
													SELECT
													    list_pack(
													        struct_pack(
													            struct_string_field := StructStringField::VARCHAR,
													            struct_int_field := StructIntField::INTEGER
													        )
													    ) AS struct_array_field
													FROM
													    json_data;



												NEXT

											CREATE TABLE json_table (data JSON);
											INSERT INTO json_table VALUES ('{"StructArrayField": [{"StructStringField": "StringValue1", "StructIntField": 1}, {"StructStringField": "StringValue2", "StructIntField": 2}]}');
											SELECT UNNEST(data->'$.StructArrayField[*]') FROM json_table;
											SELECT
												list_pack(
													struct_pack(
														struct_string_field := StructStringField::VARCHAR,
														struct_int_field := StructIntField::INTEGER
													)
												) AS struct_array_field
											FROM
												(SELECT UNNEST(data->'$.StructArrayField[*]') FROM json_table);

						TRIES

										WITH sl as (SELECT UNNEST(data->'$.StructArrayField[*]') FROM json_table)
								        SELECT sl->'$.StructStringField' as StructStringField, sl->'$.StructIntField' as StructIntField FROM sl

										WITH sl as (SELECT UNNEST(data->'$.StructArrayField[*]') FROM json_table)
								        SELECT * from sl;

										WITH sl as (SELECT UNNEST(data->'$.StructArrayField[*]') FROM json_table)
								        SELECT  $->StructStringField from sl;

							-- getting there
							-- Create the table and insert the JSON data
							CREATE TABLE json_table (data JSON);

							INSERT INTO json_table VALUES ('{"StructArrayField": [{"StructStringField": "StringValue1", "StructIntField": 1}, {"StructStringField": "StringValue2", "StructIntField": 2}]}');

							-- Use UNNEST and then extract individual fields
							WITH sl AS (
							    SELECT UNNEST(data->'$.StructArrayField[*]') AS json_struct
							    FROM json_table
							)

							SELECT
							    json_extract(json_struct, '$.StructStringField') AS StructStringField,
							    json_extract(json_struct, '$.StructIntField') AS StructIntField
							FROM sl;

						CREATE TABLE json_table (data JSON);

									INSERT INTO json_table VALUES ('{"StructArrayField": [{"StructStringField": "StringValue1", "StructIntField": 1}, {"StructStringField": "StringValue2", "StructIntField": 2}]}');

									-- Use UNNEST and then extract individual fields
									WITH sl AS (
									    SELECT UNNEST(data->'$.StructArrayField[*]') AS json_struct
									    FROM json_table
									)

									SELECT
									    json_extract(json_struct, '$.StructStringField') AS StructStringField,
									    json_extract(json_struct, '$.StructIntField') AS StructIntField
									FROM sl;


					READING FROM TABLE
					     WITH sl AS (
				      SELECT
				          UNNEST(StructArrayField) AS json_struct
				      FROM read_json_auto('/Users/kai/Dev/github/turbot/tailpipe/test_json/1.jsonl')
				  )
				  SELECT
				      json_extract(json_struct, '$.StructStringField') AS StructStringField,
				      json_extract(json_struct, '$.StructIntField') AS StructIntField
				  FROM sl;




			WITH LIST PACK
			READING FROM TABLE
				 WITH sl AS (
					  SELECT
						  UNNEST(StructArrayField) AS json_struct
					  FROM read_json_auto('/Users/kai/Dev/github/turbot/tailpipe/test_json/1.jsonl')
				  ), json_data AS (
				  SELECT
					  json_extract(json_struct, '$.StructStringField') AS StructStringField,
					  json_extract(json_struct, '$.StructIntField') AS StructIntField
				  FROM sl)



				-- Use list and struct_pack to construct a list of structs
				SELECT
					list_pack(
						struct_pack(
							struct_string_field := StructStringField::VARCHAR,
							struct_int_field := StructIntField::INTEGER
						)
					) AS struct_array_field
				FROM
					json_data;



		*/

		/*
					READING FROM TABLE
						 WITH sl AS (
							  SELECT

								  UNNEST(StructArrayField) AS json_struct
							  FROM read_json_auto('/Users/kai/Dev/github/turbot/tailpipe/test_json/1.jsonl')
						  ), json_data AS (
						  SELECT
							  json_extract(json_struct, '$.StructStringField') AS StructStringField,
							  json_extract(json_struct, '$.StructIntField') AS StructIntField
						  FROM sl)



						-- Use list and struct_pack to construct a list of structs
						SELECT
							list_pack(
								struct_pack(
									struct_string_field := StructStringField::VARCHAR,
									struct_int_field := StructIntField::INTEGER
								)
							) AS struct_array_field
						FROM
							json_data;


			READING FROM TABLE
						 WITH sl AS (
							  SELECT UNNEST(StructArrayField) AS StructArrayField
							  FROM read_json_auto('/Users/kai/Dev/github/turbot/tailpipe/test_json/1.jsonl')
						  ), json_data AS (
						  SELECT
							  json_extract(StructArrayField, '$.StructStringField') AS StructArrayField_StructStringField,
							  json_extract(StructArrayField, '$.StructIntField') AS StructArrayField_StructIntField
						  FROM sl)



						-- Use list and struct_pack to construct a list of structs
						SELECT
							list_pack(
								struct_pack(
									struct_string_field := StructArrayField_StructStringField::VARCHAR,
									struct_int_field := StructArrayField_StructIntField::INTEGER
								)
							) AS struct_array_field
						FROM
							json_data;



		*/

		// just unnest - we will add a clause to extract the fields
		return fmt.Sprintf("%sUNNEST(%s) AS %s", tab, column.SourceName, column.SourceName)
		//
		//var str strings.Builder
		//str.WriteString(fmt.Sprintf("%slist_pack (\n%s\tstruct_pack(\n", tab, tab))
		//
		//for j, nestedColumn := range column.ChildFields {
		//	if j > 0 {
		//		str.WriteString(",\n")
		//	}
		//	str.WriteString(fmt.Sprintf("%s", getTypeSqlForStructField(nestedColumn, column.SourceName, tabs+2)))
		//}
		//str.WriteString(fmt.Sprintf("\n%s)\n%s) AS %s", tab, tab, column.ColumnName))
		//return str.String()

	case "STRUCT":
		//  struct_pack(
		//  <StructColumnName> := <ParentSourceName>.<SourceName>::<Type>,
		//  ...) AS <ColumnName>

		var str strings.Builder
		str.WriteString(fmt.Sprintf("%sstruct_pack(\n", tab))
		for j, nestedColumn := range column.ChildFields {
			if j > 0 {
				str.WriteString(",\n")
			}
			str.WriteString(fmt.Sprintf("%s", getTypeSqlForStructField(nestedColumn, column.SourceName, tabs+1)))
		}
		str.WriteString(fmt.Sprintf("\n%s) AS %s", tab, column.ColumnName))
		return str.String()
	default:
		//<SourceName>::<Type> AS <ColumnName>
		return fmt.Sprintf("%s%s::%s AS %s", tab, column.SourceName, column.Type, column.ColumnName)
	}

}

// return the sql line to pack the given field as a struct
// this will use ChildFields to build a struct_pack command
func getTypeSqlForStructField(column *schema.ColumnSchema, parentName string, tabs int) string {
	tab := strings.Repeat("\t", tabs)
	switch column.Type {
	//case "MAP":
	//	// TODO

	case "STRUCT":
		var str strings.Builder
		str.WriteString(fmt.Sprintf("%s%s := struct_pack(\n", tab, column.ColumnName))
		for j, nestedColumn := range column.ChildFields {
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
