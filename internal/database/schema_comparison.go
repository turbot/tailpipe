package database

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"strings"
)

// TableSchemaStatus represents the status of a table schema comparison
// this is not used at present but will be used when we implement ducklake schema evolution handling
// It indicates whether the table exists, whether the schema matches, whether it can be migrated by ducklake
type TableSchemaStatus struct {
	TableExists   bool
	SchemaMatches bool
	CanMigrate    bool
	SchemaDiff    string
}

// NewTableSchemaStatusFromComparison compares an existing schema with a conversion schema
// and returns a TableSchemaStatus indicating whether they match, can be migrated, and the differences
func NewTableSchemaStatusFromComparison(existingSchema map[string]schema.ColumnSchema, conversionSchema schema.ConversionSchema) TableSchemaStatus {
	var diffParts []string
	canMigrate := true

	// Create map of new schema for quick lookup
	newSchemaMap := make(map[string]*schema.ColumnSchema)
	for _, column := range conversionSchema.Columns {
		newSchemaMap[column.ColumnName] = column
	}

	// Check for removed columns
	for existingColName := range existingSchema {
		if _, exists := newSchemaMap[existingColName]; !exists {
			diffParts = append(diffParts, fmt.Sprintf("- column %s removed", existingColName))
			canMigrate = false
		}
	}

	// Check for new/modified columns
	hasNewColumns := false
	for _, column := range conversionSchema.Columns {
		existingCol, ok := existingSchema[column.ColumnName]
		if !ok {
			diffParts = append(diffParts, fmt.Sprintf("+ column %s added (%s)", column.ColumnName, column.Type))
			hasNewColumns = true
			continue
		}

		if existingCol.Type != column.Type {
			diffParts = append(diffParts, fmt.Sprintf("~ column %s type changed: %s → %s",
				column.ColumnName, existingCol.Type, column.Type))
			canMigrate = false
		}
	}

	matches := len(diffParts) == 0
	if !matches && canMigrate {
		canMigrate = hasNewColumns // Only true if we only have additive changes
	}

	return TableSchemaStatus{
		TableExists:   true,
		SchemaMatches: matches,
		CanMigrate:    canMigrate,
		SchemaDiff:    strings.Join(diffParts, "\n"),
	}
}
