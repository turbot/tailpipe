package parquet

import (
	"fmt"
	"strings"

	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// determine whether we have a ducklake table for this table, and if so, whether it needs schema updating
func (w *Converter) ensureDuckLakeTable(tableName string) error {
	query := fmt.Sprintf("select exists (select 1 from information_schema.tables where table_name = '%s')", tableName)
	var exists bool
	if err := w.db.QueryRow(query).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return w.createDuckLakeTable(tableName)
	}
	return nil
}

// createDuckLakeTable creates a DuckLake table based on the ConversionSchema
func (w *Converter) createDuckLakeTable(tableName string) error {

	// Generate the CREATE TABLE SQL
	createTableSQL := w.buildCreateDucklakeTableSQL(tableName)

	// Execute the CREATE TABLE statement
	_, err := w.db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	// Set partitioning using ALTER TABLE
	// TODO need to investigate impact of ordering issues wrt to merge_adjacent files etc https://github.com/turbot/tailpipe/issues/503
	//partitionColumns := []string{constants.TpPartition, constants.TpIndex, constants.TpDate}
	//partitionColumns := []string{constants.TpPartition, constants.TpIndex, fmt.Sprintf("month(%s)", constants.TpTimestamp)}
	// partition by the year and month
	partitionColumns := []string{constants.TpPartition, constants.TpIndex, fmt.Sprintf("year(%s)", constants.TpTimestamp), fmt.Sprintf("month(%s)", constants.TpTimestamp)}
	alterTableSQL := fmt.Sprintf(`alter table "%s" set partitioned by (%s);`,
		tableName,
		strings.Join(partitionColumns, ", "))

	_, err = w.db.Exec(alterTableSQL)
	if err != nil {
		return fmt.Errorf("failed to set partitioning for table %s: %w", tableName, err)
	}

	return nil
}

// buildCreateDucklakeTableSQL generates the CREATE TABLE SQL statement based on the ConversionSchema
func (w *Converter) buildCreateDucklakeTableSQL(tableName string) string {
	// Build column definitions in sorted order
	var columnDefinitions []string
	for _, column := range w.conversionSchema.Columns {
		columnDef := w.buildColumnDefinition(column)
		columnDefinitions = append(columnDefinitions, columnDef)
	}

	return fmt.Sprintf(`create table if not exists "%s" (
%s
);`,
		tableName,
		strings.Join(columnDefinitions, ",\n"))
}

// buildColumnDefinition generates the SQL definition for a single column
func (w *Converter) buildColumnDefinition(column *schema.ColumnSchema) string {
	columnName := fmt.Sprintf("\"%s\"", column.ColumnName)

	// Handle different column types
	switch column.Type {
	case "struct":
		// For struct types, we need to build the struct definition
		structDef := w.buildStructDefinition(column)
		return fmt.Sprintf("\t%s %s", columnName, structDef)
	case "json":
		// json type
		return fmt.Sprintf("\t%s json", columnName)
	default:
		// For scalar types, just use the type directly (lower case)
		return fmt.Sprintf("\t%s %s", columnName, strings.ToLower(column.Type))
	}
}

// buildStructDefinition generates the SQL struct definition for a struct column
func (w *Converter) buildStructDefinition(column *schema.ColumnSchema) string {
	if len(column.StructFields) == 0 {
		return "struct"
	}

	var fieldDefinitions []string
	for _, field := range column.StructFields {
		fieldName := fmt.Sprintf("\"%s\"", field.ColumnName)
		fieldType := strings.ToLower(field.Type)

		if field.Type == "struct" {
			// Recursively build nested struct definition
			nestedStruct := w.buildStructDefinition(field)
			fieldDefinitions = append(fieldDefinitions, fmt.Sprintf("%s %s", fieldName, nestedStruct))
		} else {
			fieldDefinitions = append(fieldDefinitions, fmt.Sprintf("%s %s", fieldName, fieldType))
		}
	}

	return fmt.Sprintf("struct(%s)", strings.Join(fieldDefinitions, ", "))
}

// TODO #DL is this code needed - look at schema change detection
//  https://github.com/turbot/tailpipe/issues/481
//func (w *Converter) CheckTableSchema(db *sql.DB, tableName string, conversionSchema schema.ConversionSchema) (TableSchemaStatus, error) {
//	// Check if table exists
//	exists, err := w.tableExists(db, tableName)
//	if err != nil {
//		return TableSchemaStatus{}, err
//	}
//
//	if !exists {
//		return TableSchemaStatus{}, nil
//	}
//
//	// Get existing schema
//	existingSchema, err := w.getTableSchema(db, tableName)
//	if err != nil {
//		return TableSchemaStatus{}, fmt.Errorf("failed to retrieve schema: %w", err)
//	}
//
//	// Use constructor to create status from comparison
//	diff := NewTableSchemaStatusFromComparison(existingSchema, conversionSchema)
//	return diff, nil
//}
//
//func (w *Converter) tableExists(db *sql.DB, tableName string) (bool, error) {
//	query := fmt.Sprintf("select exists (select 1 from information_schema.tables where table_name = '%s')", tableName)
//	var exists int
//	if err := db.QueryRow(query).Scan(&exists); err != nil {
//		return false, err
//	}
//	return exists == 1, nil
//}

//func (w *Converter) getTableSchema(db *sql.DB, tableName string) (map[string]schema.ColumnSchema, error) {
//	query := fmt.Sprintf("pragma table_info(%s);", tableName)
//	rows, err := db.Query(query)
//	if err != nil {
//		return nil, err
//	}
//	defer rows.Close()
//
//	schemaMap := make(map[string]schema.ColumnSchema)
//	for rows.Next() {
//		var name, dataType string
//		var notNull, pk int
//		var dfltValue sql.NullString
//
//		if err := rows.Scan(&name, &dataType, &notNull, &dfltValue, &pk); err != nil {
//			return nil, err
//		}
//
//		schemaMap[name] = schema.ColumnSchema{
//			ColumnName: name,
//			Type:       dataType,
//		}
//	}
//
//	return schemaMap, nil
//}
