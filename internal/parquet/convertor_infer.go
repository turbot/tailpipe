package parquet

import (
	"encoding/json"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	"github.com/turbot/tailpipe/internal/database"
	"log"
	"path/filepath"
)

// populate the ConversionSchema
// determine if we have a full schema yet and if not infer from the chunk
func (w *Converter) buildConversionSchema(executionID string, chunk int32) error {

	// if table schema is already complete, we can skip the inference and just populate the conversionSchema
	// complete means that we have types for all columns in the table schema, and we are not mapping any source columns
	if w.tableSchema.Complete() {
		w.conversionSchema = schema.NewConversionSchema(w.tableSchema)
		return nil
	}

	// do the inference
	conversionSchema, err := w.inferConversionSchema(executionID, chunk)
	if err != nil {
		return fmt.Errorf("failed to infer conversionSchema from first JSON file: %w", err)
	}

	w.conversionSchema = conversionSchema

	// now validate the conversionSchema is complete - we should have types for all columns
	// (if we do not that indicates a custom table definition was used which does not specify types for all optional fields -
	// this should have caused a config validation error earlier on
	return w.conversionSchema.EnsureComplete()
}

func (w *Converter) inferConversionSchema(executionId string, chunkNumber int32) (*schema.ConversionSchema, error) {
	jsonFileName := table.ExecutionIdToJsonlFileName(executionId, chunkNumber)
	filePath := filepath.Join(w.sourceDir, jsonFileName)

	inferredSchema, err := w.InferSchemaForJSONLFile(filePath)
	if err != nil {
		return nil, err
	}
	return schema.NewConversionSchemaWithInferredSchema(w.tableSchema, inferredSchema), nil
}

func (w *Converter) InferSchemaForJSONLFile(filePath string) (*schema.TableSchema, error) {
	// TODO figure out why we need this hack - trying 2 different methods
	inferredSchema, err := w.inferSchemaForJSONLFileWithDescribe(filePath)
	if err != nil {
		inferredSchema, err = w.inferSchemaForJSONLFileWithJSONStructure(filePath)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to infer conversionSchema from JSON file: %w", err)
	}
	inferredSchema.NormaliseColumnTypes()
	return inferredSchema, nil
}

// inferSchemaForJSONLFileWithJSONStructure infers the schema of a JSONL file using DuckDB
// it uses 2 different queries as depending on the data, one or the other has been observed to work
// (needs investigation)
func (w *Converter) inferSchemaForJSONLFileWithJSONStructure(filePath string) (*schema.TableSchema, error) {
	// Open DuckDB connection
	db, err := database.NewDuckDb()
	if err != nil {
		log.Fatalf("failed to open DuckDB connection: %v", err)
	}
	defer db.Close()

	// Query to infer schema using json_structure
	query := `
		select json_structure(json)::varchar as schema
		from read_json_auto(?)
		limit 1;
	`

	var schemaStr string
	err = db.QueryRow(query, filePath).Scan(&schemaStr)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Parse the schema JSON
	var fields map[string]string
	if err := json.Unmarshal([]byte(schemaStr), &fields); err != nil {
		return nil, fmt.Errorf("failed to parse schema JSON: %w", err)
	}

	// Convert to TableSchema
	res := &schema.TableSchema{
		Columns: make([]*schema.ColumnSchema, 0, len(fields)),
	}

	// Convert each field to a column schema
	for name, typ := range fields {
		res.Columns = append(res.Columns, &schema.ColumnSchema{
			SourceName: name,
			ColumnName: name,
			Type:       typ,
		})
	}

	return res, nil
}

func (w *Converter) inferSchemaForJSONLFileWithDescribe(filePath string) (*schema.TableSchema, error) {

	// Open DuckDB connection
	db, err := database.NewDuckDb()
	if err != nil {
		log.Fatalf("failed to open DuckDB connection: %v", err)
	}
	defer db.Close()

	// Use DuckDB to describe the schema of the JSONL file
	query := `SELECT column_name, column_type FROM (DESCRIBE (SELECT * FROM read_json_auto(?)))`

	rows, err := db.Query(query, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to query JSON schema: %w", err)
	}
	defer rows.Close()

	var res = &schema.TableSchema{}

	// Read the results
	for rows.Next() {
		var name, dataType string
		err := rows.Scan(&name, &dataType)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		// Append inferred columns to the schema
		res.Columns = append(res.Columns, &schema.ColumnSchema{
			SourceName: name,
			ColumnName: name,
			Type:       dataType,
		})
	}

	// Check for any errors from iterating over rows
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed during rows iteration: %w", err)
	}

	return res, nil
}

func (w *Converter) detectSchemaChange(filePath string) error {
	inferredChunksSchema, err := w.InferSchemaForJSONLFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to infer schema from JSON file: %w", err)
	}
	// the conversion schema is the full schema for the table that we have alreadf inferred
	conversionSchemaMap := w.conversionSchema.AsMap()
	// the table schema is the (possibly partial) schema which was defined in config - we use this to exclude columns
	// which have a type specified
	tableSchemaMap := w.tableSchema.AsMap()
	// Compare the inferred schema with the existing conversionSchema
	var changedColumns []ColumnSchemaChange
	for _, col := range inferredChunksSchema.Columns {
		// if the table schema definition specifies a type for this column, ignore the columns (as we will use the defined type)
		// we are only interested in a type change if the column is not defined in the table schema
		if columnDef, ok := tableSchemaMap[col.ColumnName]; ok {
			if columnDef.Type != "" {
				// if the column is defined in the table schema, ignore it
				continue
			}
		}

		existingCol, exists := conversionSchemaMap[col.SourceName]
		if exists && col.Type != existingCol.Type {
			changedColumns = append(changedColumns, ColumnSchemaChange{
				Name:    col.SourceName,
				OldType: existingCol.Type,
				NewType: col.Type,
			})
		}
	}
	if len(changedColumns) > 0 {
		return &SchemaChangeError{ChangedColumns: changedColumns}
	}
	return nil
}
