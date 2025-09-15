package database

import (
	"fmt"
	"strings"

	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// determine whether we have a ducklake table for this table, and if so, whether it needs schema updating
func EnsureDuckLakeTable(columns []*schema.ColumnSchema, db *DuckDb, tableName string) error {
	query := fmt.Sprintf("select exists (select 1 from information_schema.tables where table_name = '%s')", tableName)
	var exists bool
	if err := db.QueryRow(query).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return createDuckLakeTable(columns, db, tableName)
	}
	return nil
}

// createDuckLakeTable creates a DuckLake table based on the ConversionSchema
func createDuckLakeTable(columns []*schema.ColumnSchema, db *DuckDb, tableName string) error {

	// Generate the CREATE TABLE SQL
	createTableSQL := buildCreateDucklakeTableSQL(columns, tableName)

	// Execute the CREATE TABLE statement
	_, err := db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	// Set partitioning using ALTER TABLE
	// partition by the partition, index, year and month
	partitionColumns := []string{constants.TpPartition, constants.TpIndex, fmt.Sprintf("year(%s)", constants.TpTimestamp), fmt.Sprintf("month(%s)", constants.TpTimestamp)}
	alterTableSQL := fmt.Sprintf(`alter table "%s" set partitioned by (%s);`,
		tableName,
		strings.Join(partitionColumns, ", "))

	_, err = db.Exec(alterTableSQL)
	if err != nil {
		return fmt.Errorf("failed to set partitioning for table %s: %w", tableName, err)
	}

	return nil
}

// buildCreateDucklakeTableSQL generates the CREATE TABLE SQL statement based on the ConversionSchema
func buildCreateDucklakeTableSQL(columns []*schema.ColumnSchema, tableName string) string {
	// Build column definitions in sorted order
	var columnDefinitions []string
	for _, column := range columns {
		columnDef := buildColumnDefinition(column)
		columnDefinitions = append(columnDefinitions, columnDef)
	}

	return fmt.Sprintf(`create table if not exists "%s" (
%s
);`,
		tableName,
		strings.Join(columnDefinitions, ",\n"))
}

// buildColumnDefinition generates the SQL definition for a single column
func buildColumnDefinition(column *schema.ColumnSchema) string {
	columnName := fmt.Sprintf("\"%s\"", column.ColumnName)

	// Handle different column types
	switch column.Type {
	case "struct":
		// For struct types, we need to build the struct definition
		structDef := buildStructDefinition(column)
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
func buildStructDefinition(column *schema.ColumnSchema) string {
	if len(column.StructFields) == 0 {
		return "struct"
	}

	var fieldDefinitions []string
	for _, field := range column.StructFields {
		fieldName := fmt.Sprintf("\"%s\"", field.ColumnName)
		fieldType := strings.ToLower(field.Type)

		if field.Type == "struct" {
			// Recursively build nested struct definition
			nestedStruct := buildStructDefinition(field)
			fieldDefinitions = append(fieldDefinitions, fmt.Sprintf("%s %s", fieldName, nestedStruct))
		} else {
			fieldDefinitions = append(fieldDefinitions, fmt.Sprintf("%s %s", fieldName, fieldType))
		}
	}

	return fmt.Sprintf("struct(%s)", strings.Join(fieldDefinitions, ", "))
}
