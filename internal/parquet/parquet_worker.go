package parquet

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/plugin"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

	// Create a view from the JSONL file
	//createViewQuery := fmt.Sprintf(`CREATE VIEW json_view AS SELECT * FROM read_json_auto('%s', format='newline_delimited');`, jsonlFilePath)
	//_, err = w.db.Exec(createViewQuery)
	//if err != nil {
	//	return fmt.Errorf("failed to create view from JSONL file: %w", err)
	//}
	//defer func() {
	//	// drop the view
	//	if _, dropErr := w.db.Exec("DROP VIEW IF EXISTS json_view;"); dropErr != nil {
	//		err = errors.Join(err, fmt.Errorf("failed to drop view: %w", dropErr))
	//	}
	//}()

	// determine the root based on the collection type
	fileRoot := w.getParquetFileRoot(collectionType)
	if err := os.MkdirAll(filepath.Dir(fileRoot), 0755); err != nil {
		return fmt.Errorf("failed to create parquet folder: %w", err)
	}

	selectQuery := w.buildSelectQuery(schema, jsonlFilePath)
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

func (w *parquetConversionWorker) buildSelectQuery(schema *schema.RowSchema, jsonlFilePath string) string {
	var str strings.Builder

	/*

		SELECT
		    tpIps::VARCHAR[] AS tp_ips,
		    tpUsernames::VARCHAR[] AS tp_usernames,
		    struct_pack(
		        type_feld := userIdentity.typeField::VARCHAR,
		        session_context := userIdentity.sessionContext::VARCHAR,
		        invoked_by := userIdentity.invokedBy::JSON,
		        identity_provider := userIdentity.identityProvider::VARCHAR,
		        nested := struct_pack(
		            type_field := userIdentity.nested.typeField::VARCHAR,
		            identity_provider := userIdentity.nested.identityProvider::JSON
		        )
		    ) AS user_identity
		FROM read_json_auto('/Users/kai/Dev/github/turbot/tailpipe/test_json/1.jsonl');

	*/
	str.WriteString("SELECT\n")
	//for i, column := range schema.Columns {
	//	if i > 0 {
	//		str.WriteString(",\n")
	//	}
	//	if column.Type == "struct" {
	//		str.WriteString("    struct_pack(\n")
	//		for j, nestedColumn := range column.NestedColumns {
	//			if j > 0 {
	//				str.WriteString(",\n")
	//			}
	//			str.WriteString(fmt.Sprintf("        %s := %s::%s", nestedColumn.ColumnName, nestedColumn.SourceName, nestedColumn.Type))
	//		}
	//		str.WriteString(fmt.Sprintf("\n    ) AS %s", column.ColumnName))
	//		continue
	//	}
	//
	//	str.WriteString(fmt.Sprintf("\n    %s::%s AS %s", column.SourceName, column.Type, column.ColumnName))
	//}
	//str.WriteString(fmt.Sprintf("\n  FROM read_json_auto('%s', format='newline_delimited')", jsonlFilePath))

	return str.String()

}
