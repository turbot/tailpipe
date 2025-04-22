package parquet

import (
	"fmt"
	"strings"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// TODO: review this function & add comments: https://github.com/turbot/tailpipe/issues/305
func buildViewQuery(tableSchema *schema.TableSchema) string {
	// ensure the schema types are normalised
	tableSchema.NormaliseColumnTypes()

	var structSliceColumns []*schema.ColumnSchema

	// first build the columns to select from the jsonl file
	var columnStrings strings.Builder
	for i, column := range tableSchema.Columns {
		if i > 0 {
			columnStrings.WriteString(",\n")
		}

		columnStrings.WriteString(getSqlForField(column, 1))
		if column.Type == "struct[]" {
			structSliceColumns = append(structSliceColumns, column)
		}
		// TODO take nested struct arays into account
		//for _, nestedColumn := range column.StructFields {
		//	if nestedColumn.Type == "struct[]" {
		//		structSliceColumns = append(structSliceColumns, nestedColumn)
		//	}
		//
		//}
	}

	// build column definitions
	columnDefinitions := getReadJSONColumnDefinitions(tableSchema)

	columnStrings.WriteString(fmt.Sprintf(`
from
	read_ndjson(
		'%%s',
%s
	))`, helpers.Tabify(columnDefinitions, "\t\t")))

	// if there are no struct[] fields, we are done - just add the select at the start
	if len(structSliceColumns) == 0 {
		// note: extra select wrapper is used to allow for wrapping query before filter is applied so filter can use struct fields with dot-notation
		return fmt.Sprintf("select * from(select\n%s", columnStrings.String())
	}

	// TODO: Currently we don't support []struct so this code never gets hit. https://github.com/turbot/tailpipe-plugin-sdk/issues/55
	// if there are struct[] fields, we need to build a more complex query

	// add row number in case of potential grouping
	var str strings.Builder
	// note: extra select wrapper is used to allow for wrapping query before filter is applied so filter can use struct fields with dot-notation
	str.WriteString("select * from(select\n")
	str.WriteString("\trow_number() over () as rowid,\n")
	str.WriteString(columnStrings.String())

	// build the struct slice query
	return getViewQueryForStructSlices(str.String(), tableSchema, structSliceColumns)

}

// return the column definitions for the row schema, in the format required for the duck db read_json_auto function
func getReadJSONColumnDefinitions(rowSchema *schema.TableSchema) string {
	// columns = {BooleanField: 'boolean', BooleanField2: 'boolean', BooleanField3: 'boolean'})
	var str strings.Builder
	str.WriteString("columns = {")
	for i, column := range rowSchema.Columns {
		if i > 0 {
			str.WriteString(", ")
		}
		str.WriteString(fmt.Sprintf(`
	"%s": '%s'`, column.SourceName, column.FullType()))
	}
	str.WriteString("\n}")
	return str.String()
}

func getViewQueryForStructSlices(q string, rowSchema *schema.TableSchema, structSliceColumns []*schema.ColumnSchema) string {
	var str strings.Builder

	/* this is the what we want

	with raw as (
	    select
	        row_number() OVER () as rowid,
	        "StructArrayField" as "struct_array_field",
	        "IntField" as "int_field",
	        "StringField" as "string_field",
	        "FloatField" as "float_field",
	        "BooleanField" as "boolean_field",
	        "IntArrayField" as "int_array_field",
	        "StringArrayArrayField" as "string_array_field",
	        "FloatArrayField" as "float_array_field",
	        "BooleanArrayField" as "boolean_array_field"
	    from
	        read_ndjson(
	            '/Users/kai/Dev/github/turbot/tailpipe/internal/parquet/buildViewQuery_test_data/1.jsonl',
	            columns = {
	                "StructArrayField": 'struct("StructStringField" varchar, "StructIntField" integer)[]',
	                "IntField": 'integer',
	                "StringField": 'varchar',
	                "FloatField": 'float',
	                "BooleanField": 'boolean',
	                "IntArrayField": 'integer[]',
	                "StringArrayArrayField": 'varchar[]',
	                "FloatArrayField": 'float[]',
	                "BooleanArrayField": 'boolean[]'
	            }
	        )
	), unnest_struct_array_field as (
	    select
	        rowid,
	        unnest(coalesce("struct_array_field", array[]::struct("StructStringField" varchar, "StructIntField" integer)[])::struct("StructStringField" varchar, "StructIntField" integer)[]) as struct_array_field
	    from
	        raw
	), rebuild_unnest_struct_array_field as (
	    select
	        rowid,
	        struct_array_field->>'StructStringField' as StructArrayField_StructStringField,
	        struct_array_field->>'StructIntField' as StructArrayField_StructIntField
	    from
	        unnest_struct_array_field
	), grouped_unnest_struct_array_field as (
	    select
	        rowid,
	        array_agg(struct_pack(
	            struct_string_field := StructArrayField_StructStringField::varchar,
	            struct_int_field := StructArrayField_StructIntField::integer
	        )) as struct_array_field
	    from
	        rebuild_unnest_struct_array_field
	    GROUP BY
	        rowid
	)
	select
	    COALESCE(joined_struct_array_field.struct_array_field, NULL) as struct_array_field,
	    raw.int_field,
	    raw.string_field,
	    raw.float_field,
	    raw.boolean_field,
	    raw.int_array_field,
	    raw.string_array_field,
	    raw.float_array_field,
	    raw.boolean_array_field
	from
	    raw
	LEFT JOIN
	    grouped_unnest_struct_array_field joined_struct_array_field on raw.rowid = joined_struct_array_field.rowid;
	*/

	/* 	with raw as (

	 */
	str.WriteString("with raw as (\n")
	str.WriteString(helpers.Tabify(q, "\t"))
	str.WriteString("\n)")

	// for every struct[] field, we need to add various CTEs
	for _, structSliceCol := range structSliceColumns {

		/*
			, unnest_struct_array_field as (
			    select
			        rowid,
		*/
		unnestName := fmt.Sprintf(`unnest_%s`, structSliceCol.ColumnName)

		str.WriteString(fmt.Sprintf(`, %s as (
    select
        rowid,`, unnestName))

		/*
			unnest(coalesce("struct_array_field", array[]::struct("StructStringField" varchar, "StructIntField" integer)[])::struct("StructStringField" varchar, "StructIntField" integer)[]) as struct_array_field
		*/

		str.WriteString(fmt.Sprintf(`
		unnest(coalesce("%s", array[]::%s)::%s) as %s`, structSliceCol.ColumnName, structSliceCol.FullType(), structSliceCol.FullType(), structSliceCol.ColumnName))

		/*
		   	from
		   		raw
		   )
		*/

		str.WriteString(`
	from
		raw
)`)

		/*
		   , rebuild_unnest_struct_array_field as (
		      select
		   	   rowid,
		*/
		rebuildName := fmt.Sprintf(`rebuild_%s`, unnestName)

		str.WriteString(fmt.Sprintf(`, %s as (
	select
		rowid`, rebuildName))

		// loop over fields in the struct
		/*
			        struct_array_field->>'StructStringField' as StructArrayField_StructStringField,
			)
		*/
		for _, structField := range structSliceCol.StructFields {
			str.WriteString(",\n")
			str.WriteString(fmt.Sprintf(`		%s->>'%s' as %s_%s`, structSliceCol.ColumnName, structField.SourceName, structSliceCol.SourceName, structField.SourceName))
		}
		/*
		   	from
		   		unnest_struct_array_field
		   )
		*/

		str.WriteString(fmt.Sprintf(`
	from
		%s
)`, unnestName))

		/*
		      , grouped_unnest_struct_array_field as (
		      	    select
		      	        rowid,
		      	        array_agg(struct_pack(
		   				struct_string_field := StructArrayField_StructStringField::varchar,
		      	            struct_int_field := StructArrayField_StructIntField::integer
		      	        )) as struct_array_field
		      	    from
		      	        rebuild_unnest_struct_array_field
		      	    GROUP BY
		      	        rowid
		      	)

		*/
		groupedName := fmt.Sprintf(`grouped_%s`, unnestName)
		str.WriteString(fmt.Sprintf(`, %s as (`, groupedName))
		str.WriteString(`
	select
		rowid,	
		array_agg(struct_pack(
`)
		// loop over struct
		for i, structField := range structSliceCol.StructFields {
			if i > 0 {
				str.WriteString(",\n")
			}
			str.WriteString(fmt.Sprintf(`				%s := %s_%s::%s`, structField.ColumnName, structSliceCol.SourceName, structField.SourceName, structField.Type))
		}
		str.WriteString(fmt.Sprintf(`
		)) as %s	
	from
		%s	
	GROUP BY
		rowid	
)`, structSliceCol.ColumnName, rebuildName))

	}
	// build the final select
	/*
		select
			    COALESCE(joined_struct_array_field.struct_array_field, NULL) as struct_array_field,
			    raw.int_field,
			    raw.string_field,
			    raw.float_field,
			    raw.boolean_field,
			    raw.int_array_field,
			    raw.string_array_field,
			    raw.float_array_field,
			    raw.boolean_array_field
			from
			    raw
			LEFT JOIN
			    grouped_unnest_struct_array_field joined_struct_array_field on raw.rowid = joined_struct_array_field.rowid;
	*/
	// build list of coalesce fields and join fields
	var coalesceFields strings.Builder
	var leftJoins strings.Builder
	for i, column := range structSliceColumns {
		if i > 0 {
			coalesceFields.WriteString(",\n")
			leftJoins.WriteString("\n")
		}
		joinedName := fmt.Sprintf(`joined_%s`, column.ColumnName)
		groupedName := fmt.Sprintf(`grouped_unnest_%s`, column.ColumnName)

		coalesceFields.WriteString(fmt.Sprintf(`	coalesce(%s.%s, null) as %s`, joinedName, column.ColumnName, column.ColumnName))
		leftJoins.WriteString(fmt.Sprintf(`LEFT JOIN
	%s %s on raw.rowid = %s.rowid`, groupedName, joinedName, joinedName))

	}

	// now construct final select
	str.WriteString(fmt.Sprintf(`
select
%s`, coalesceFields.String()))

	for _, column := range rowSchema.Columns {
		if column.Type != "struct[]" {
			str.WriteString(",\n")
			str.WriteString(fmt.Sprintf(`	raw.%s`, column.ColumnName))
		}
	}
	str.WriteString(fmt.Sprintf(`
from
	raw	
%s`, leftJoins.String()))

	return str.String()
}

// Return the SQL line to select the given field
func getSqlForField(column *schema.ColumnSchema, tabs int) string {
	// Calculate the tab spacing
	tab := strings.Repeat("\t", tabs)

	// NOTE: we will have normalised column types to lower case
	switch column.Type {
	case "struct":
		var str strings.Builder

		// Start case logic to handle null values for the struct
		str.WriteString(fmt.Sprintf("%scase\n", tab))
		str.WriteString(fmt.Sprintf("%s\twhen \"%s\" is null then null\n", tab, column.SourceName))
		str.WriteString(fmt.Sprintf("%s\telse struct_pack(\n", tab))

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
