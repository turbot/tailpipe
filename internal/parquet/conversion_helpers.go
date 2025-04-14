package parquet

import (
	"fmt"
	"strings"

	"github.com/turbot/tailpipe-plugin-sdk/constants"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// TODO: review this function & add comments: https://github.com/turbot/tailpipe/issues/305
func buildViewQuery(tableSchema *schema.ConversionSchema) string {
	var tpIndexMapped, tpTimestampMapped bool

	// first build the select clauses - use the table def columns
	var selectClauses []string
	for _, column := range tableSchema.Columns {

		var selectClause string
		switch column.ColumnName {
		case constants.TpDate:
			// skip this column - it is derived from tp_timestamp
			continue
		case constants.TpIndex:
			tpIndexMapped = true
			selectClause = fmt.Sprintf("\tcoalesce(\"%s\", '%s') as \"%s\"", column.SourceName, schema.DefaultIndex, column.ColumnName)
		case constants.TpTimestamp:
			tpTimestampMapped = true
			// fallthrough to populate the select clasue as normal
			fallthrough
		default:
			selectClause = getSelectSqlForField(column, 1)
		}

		selectClauses = append(selectClauses, selectClause)
	}
	// default tpIndex to 'default'
	// (note - if tpIndex IS mapped, getSelectSqlForField will have added a coalesce statement to default to 'default'
	if !tpIndexMapped {
		selectClauses = append(selectClauses, fmt.Sprintf("'%s' as tp_index", schema.DefaultIndex))
	}
	// if we have a mapping for tp_timestamp, add tp_date as well
	if tpTimestampMapped {
		// Add tp_date after tp_timestamp is defined
		selectClauses = append(selectClauses, `	case
		when tp_timestamp is not null then date_trunc('day', tp_timestamp::timestamp)
	end as tp_date`)
	}

	// build column definitions - these will be passed to the read_json function
	columnDefinitions := getReadJSONColumnDefinitions(tableSchema.SourceColumns)

	// note: extra select wrapper is used to allow for wrapping query before filter is applied so filter can use struct fields with dot-notation
	return fmt.Sprintf(`select * from (select
	row_number() over () as rowid,
%s
from
	read_ndjson(
		'%%s',
	%s
	))`, strings.Join(selectClauses, ",\n"), helpers.Tabify(columnDefinitions, "\t"))
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

// Return the SQL line to select the given field
func getSelectSqlForField(column *schema.ColumnSchema, tabs int) string {
	// Calculate the tab spacing
	tab := strings.Repeat("\t", tabs)

	// If the column has a transform, use it
	if column.Transform != "" {
		// as this is going into a string format, we need to escape %
		escapedTransform := strings.ReplaceAll(column.Transform, "%", "%%")
		return fmt.Sprintf("%s%s as \"%s\"", tab, escapedTransform, column.ColumnName)
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
			str.WriteString(getTypeSqlForStructField(nestedColumn, parentName, tabs+2))
		}

		// Close struct_pack and case
		str.WriteString(fmt.Sprintf("\n%s\t)\n", tab))
		str.WriteString(fmt.Sprintf("%send as \"%s\"", tab, column.ColumnName))
		return str.String()

	case "json":
		// Convert the value using json()
		return fmt.Sprintf("%sjson(\"%s\") as \"%s\"", tab, column.SourceName, column.ColumnName)

	default:
		// Scalar fields
		return fmt.Sprintf("%s\"%s\" as \"%s\"", tab, column.SourceName, column.ColumnName)
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
