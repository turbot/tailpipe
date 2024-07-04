package parquet

import (
	"fmt"
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

func buildViewQuery(schema *schema.RowSchema, jsonlFilePath string) string {
	var str strings.Builder

	/*

		SELECT
		    tpIps::VARCHAR[] AS tp_ips,
		    tpUsernames::VARCHAR[] AS tp_usernames,
			list_pack (
					struct_pack(
						type_feld := userIdentity.typeField::VARCHAR,
						session_context := userIdentity.sessionContext::VARCHAR,
						invoked_by := userIdentity.invokedBy::JSON,
						identity_provider := userIdentity.identityProvider::VARCHAR,
						nested := struct_pack(
							type_field := userIdentity.nested.typeField::VARCHAR,
							identity_provider := userIdentity.nested.identityProvider::JSON
						)
					)
			) AS user_identity
		FROM read_json_auto('/Users/kai/Dev/github/turbot/tailpipe/test_json/1.jsonl');

	*/
	str.WriteString("SELECT\n")
	for i, column := range schema.Columns {
		if i > 0 {
			str.WriteString(",\n")
		}
		str.WriteString(fmt.Sprintf("%s", getSqlForField(column, 1)))
	}
	str.WriteString(fmt.Sprintf("\nFROM read_json_auto('%s', format='newline_delimited')", jsonlFilePath))

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

		*/
		// list_pack (
		//  	struct_pack(
		// 			 <StructColumnName> := <ParentSourceName>.<SourceName>::<Type>,
		//  		...)
		// 		) AS <ColumnName>

		// TODO DO NOT CAST FOR NOW
		return fmt.Sprintf("%s%s AS %s", tab, column.SourceName, column.ColumnName)

		var str strings.Builder
		str.WriteString(fmt.Sprintf("%slist_pack (\n%s\tstruct_pack(\n", tab, tab))

		for j, nestedColumn := range column.ChildFields {
			if j > 0 {
				str.WriteString(",\n")
			}
			str.WriteString(fmt.Sprintf("%s", getTypeSqlForStructField(nestedColumn, column.SourceName, tabs+2)))
		}
		str.WriteString(fmt.Sprintf("\n%s)\n%s) AS %s", tab, tab, column.ColumnName))
		return str.String()

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
