package database

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe/internal/config"
)

// buildReadJsonQueryFormat creates a SQL query template for reading JSONL files with DuckDB.
//
// Returns a format string with a %s placeholder for the JSON filename that gets filled in when executed.
// The query is built by constructing a select clause for each field in the conversion schema,
// adding tp_index from partition config, and applying any partition filters (e.g. date filer)
//
// Example output:
//
//	select "user_id" as "user_id", "name" as "user_name", "created_at" as "tp_timestamp",
//	       "default" as "tp_index"
//	from read_ndjson(%s, columns = {"user_id": 'varchar', "name": 'varchar', "created_at": 'timestamp'})
func buildReadJsonQueryFormat(conversionSchema *schema.ConversionSchema, partition *config.Partition) string {
	var tpTimestampMapped bool

	// first build the select clauses - use the table def columns
	var selectClauses = []string{
		"row_number() over () as row_id", // add a row_id column to use with validation
	}
	for _, column := range conversionSchema.Columns {
		var selectClause string
		switch column.ColumnName {
		case constants.TpDate:
			// skip this column - it is derived from tp_timestamp
			continue
		case constants.TpIndex:
			// NOTE: we ignore tp_index in the source data and ONLY add it based ont he default or configured value
			slog.Warn("tp_index is a reserved column name and should not be used in the source data. It will be added automatically based on the configured value.")
			// skip this column - it will be populated manually using the partition config
			continue
		case constants.TpTimestamp:
			tpTimestampMapped = true
			// fallthrough to populate the select clasue as normal
			fallthrough
		default:
			selectClause = getSelectSqlForField(column)
		}

		selectClauses = append(selectClauses, selectClause)
	}

	// add the tp_index - this is determined by the partition - it defaults to "default" but may be overridden in the partition config
	// NOTE: we DO NOT wrap the tp_index expression in quotes - that will have already been done as part of partition config validation
	selectClauses = append(selectClauses, fmt.Sprintf("\t%s as \"tp_index\"", partition.TpIndexColumn))

	// if we have a mapping for tp_timestamp, add tp_date as well
	// (if we DO NOT have tp_timestamp, the validation will fail - but we want the validation error -
	//  NOT an error when we try to select tp_date using tp_timestamp as source)
	if tpTimestampMapped {
		// Add tp_date after tp_timestamp is defined
		selectClauses = append(selectClauses, `	case
		when tp_timestamp is not null then date_trunc('day', tp_timestamp::timestamp)
	end as tp_date`)
	}

	// build column definitions - these will be passed to the read_json function
	columnDefinitions := getReadJSONColumnDefinitions(conversionSchema.SourceColumns)

	var whereClause string
	if partition.Filter != "" {
		// we need to escape the % in the filter, as it is passed to the fmt.Sprintf function
		filter := strings.ReplaceAll(partition.Filter, "%", "%%")
		whereClause = fmt.Sprintf("\nwhere %s", filter)
	}

	res := fmt.Sprintf(`select
%s
from
	read_ndjson(
		%%s,
	%s
	)%s`, strings.Join(selectClauses, ",\n"), helpers.Tabify(columnDefinitions, "\t"), whereClause)

	return res
}

// return the column definitions for the row conversionSchema, in the format required for the duck db read_json_auto function
func getReadJSONColumnDefinitions(sourceColumns []schema.SourceColumnDef) string {
	var str strings.Builder
	str.WriteString("columns = {")
	for i, column := range sourceColumns {
		if i > 0 {
			str.WriteString(", ")
		}
		str.WriteString(fmt.Sprintf(`
		"%s": '%s'`, column.Name, column.Type))
	}
	str.WriteString("\n}")
	return str.String()
}

// getSelectSqlForField builds a SELECT clause for a single field based on its schema definition.
// - If the field has a transform defined, it uses that transform expression.
// - For struct fields, it creates a struct_pack expression to properly construct the nested structure from the source JSON data.
// - All other field types are handled with simple column references.
func getSelectSqlForField(column *schema.ColumnSchema) string {

	// If the column has a transform, use it
	if column.Transform != "" {
		// as this is going into a string format, we need to escape %
		escapedTransform := strings.ReplaceAll(column.Transform, "%", "%%")
		return fmt.Sprintf("\t%s as \"%s\"", escapedTransform, column.ColumnName)
	}

	// NOTE: we will have normalised column types to lower case
	switch column.Type {
	case "struct":
		var str strings.Builder

		// Start case logic to handle null values for the struct

		str.WriteString(fmt.Sprintf("\tcase\n\t\twhen \"%s\" is null then null\n", column.SourceName))
		str.WriteString("\t\telse struct_pack(\n")

		// Add nested fields to the struct_pack
		for j, nestedColumn := range column.StructFields {
			if j > 0 {
				str.WriteString(",\n")
			}
			parentName := fmt.Sprintf("\"%s\"", column.SourceName)
			str.WriteString(getTypeSqlForStructField(nestedColumn, parentName, 3))
		}

		// Close struct_pack and case
		str.WriteString("\n\t\t)\n")
		str.WriteString(fmt.Sprintf("\tend as \"%s\"", column.ColumnName))
		return str.String()
	default:
		// Scalar fields
		return fmt.Sprintf("\t\"%s\" as \"%s\"", column.SourceName, column.ColumnName)
	}
}

// Return the SQL line to pack the given field as a struct
func getTypeSqlForStructField(column *schema.ColumnSchema, parentName string, tabs int) string {
	tab := strings.Repeat("\t", tabs)

	switch column.Type {
	case "struct":
		var str strings.Builder

		// Add case logic to handle null values for the struct
		str.WriteString(fmt.Sprintf("%s\"%s\" := case\n", tab, column.ColumnName))
		str.WriteString(fmt.Sprintf("%s\twhen %s.\"%s\" is null then null\n", tab, parentName, column.SourceName))
		str.WriteString(fmt.Sprintf("%s\telse struct_pack(\n", tab))

		// Loop through nested fields and add them to the struct_pack
		for j, nestedColumn := range column.StructFields {
			if j > 0 {
				str.WriteString(",\n")
			}
			// Use the current field as the new parent for recursion
			newParent := fmt.Sprintf("%s.\"%s\"", parentName, column.SourceName)
			str.WriteString(getTypeSqlForStructField(nestedColumn, newParent, tabs+2))
		}

		// Close struct_pack and case
		str.WriteString(fmt.Sprintf("\n%s\t)\n", tab))
		str.WriteString(fmt.Sprintf("%send", tab))
		return str.String()

	default:
		// Scalar fields
		return fmt.Sprintf("%s\"%s\" := %s.\"%s\"::%s", tab, column.ColumnName, parentName, column.SourceName, column.Type)
	}
}
