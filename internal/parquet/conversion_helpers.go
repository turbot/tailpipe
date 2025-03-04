package parquet

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe/internal/filepaths"
)

func getRowCount(db *sql.DB, destDir, fileRoot, table string) (int64, error) {
	// Build the query
	//nolint:gosec // cannot use params in parquet_file_metadata - and this is a trusted source
	rowCountQuery := fmt.Sprintf(`SELECT SUM(num_rows) FROM parquet_file_metadata('%s')`, filepaths.GetParquetFileGlobForTable(destDir, table, fileRoot))

	// Execute the query and scan the result directly
	var rowCount int64
	err := db.QueryRow(rowCountQuery).Scan(&rowCount)
	if err != nil {
		// if this is an IO error caused by no files being found, return 0
		if strings.Contains(err.Error(), "No files found") {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to query row count: %w", err)
	}
	return rowCount, nil
}

func buildViewQuery(rowSchema *schema.TableSchema) string {
	var structSliceColumns []*schema.ColumnSchema

	// first build the columns to select from the jsonl file
	var columnStrings strings.Builder
	for i, column := range rowSchema.Columns {
		if i > 0 {
			columnStrings.WriteString(",\n")
		}

		columnStrings.WriteString(getSqlForField(column, 1))
		if column.Type == "STRUCT[]" {
			structSliceColumns = append(structSliceColumns, column)
		}
		// TODO take nested struct arays into account
		//for _, nestedColumn := range column.StructFields {
		//	if nestedColumn.Type == "STRUCT[]" {
		//		structSliceColumns = append(structSliceColumns, nestedColumn)
		//	}
		//
		//}
	}

	// build column definitions
	columnDefinitions := getReadJSONColumnDefinitions(rowSchema)

	columnStrings.WriteString(fmt.Sprintf(`
FROM
	read_ndjson(
		'%%s',
%s
	)`, helpers.Tabify(columnDefinitions, "\t\t")))

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

// return the column definitions for the row schema, in the format required for the duck testDb read_json_auto function
func getReadJSONColumnDefinitions(rowSchema *schema.TableSchema) string {
	// columns = {BooleanField: 'BOOLEAN', BooleanField2: 'BOOLEAN', BooleanField3: 'BOOLEAN'})
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

	WITH raw AS (
	    SELECT
	        row_number() OVER () AS rowid,
	        "StructArrayField" AS "struct_array_field",
	        "IntField" AS "int_field",
	        "StringField" AS "string_field",
	        "FloatField" AS "float_field",
	        "BooleanField" AS "boolean_field",
	        "IntArrayField" AS "int_array_field",
	        "StringArrayArrayField" AS "string_array_field",
	        "FloatArrayField" AS "float_array_field",
	        "BooleanArrayField" AS "boolean_array_field"
	    FROM
	        read_ndjson(
	            '/Users/kai/Dev/github/turbot/tailpipe/internal/parquet/buildViewQuery_test_data/1.jsonl',
	            columns = {
	                "StructArrayField": 'STRUCT("StructStringField" VARCHAR, "StructIntField" INTEGER)[]',
	                "IntField": 'INTEGER',
	                "StringField": 'VARCHAR',
	                "FloatField": 'FLOAT',
	                "BooleanField": 'BOOLEAN',
	                "IntArrayField": 'INTEGER[]',
	                "StringArrayArrayField": 'VARCHAR[]',
	                "FloatArrayField": 'FLOAT[]',
	                "BooleanArrayField": 'BOOLEAN[]'
	            }
	        )
	), unnest_struct_array_field AS (
	    SELECT
	        rowid,
	        UNNEST(COALESCE("struct_array_field", ARRAY[]::STRUCT("StructStringField" VARCHAR, "StructIntField" INTEGER)[])::STRUCT("StructStringField" VARCHAR, "StructIntField" INTEGER)[]) AS struct_array_field
	    FROM
	        raw
	), rebuild_unnest_struct_array_field AS (
	    SELECT
	        rowid,
	        struct_array_field->>'StructStringField' AS StructArrayField_StructStringField,
	        struct_array_field->>'StructIntField' AS StructArrayField_StructIntField
	    FROM
	        unnest_struct_array_field
	), grouped_unnest_struct_array_field AS (
	    SELECT
	        rowid,
	        array_agg(struct_pack(
	            struct_string_field := StructArrayField_StructStringField::VARCHAR,
	            struct_int_field := StructArrayField_StructIntField::INTEGER
	        )) AS struct_array_field
	    FROM
	        rebuild_unnest_struct_array_field
	    GROUP BY
	        rowid
	)
	SELECT
	    COALESCE(joined_struct_array_field.struct_array_field, NULL) AS struct_array_field,
	    raw.int_field,
	    raw.string_field,
	    raw.float_field,
	    raw.boolean_field,
	    raw.int_array_field,
	    raw.string_array_field,
	    raw.float_array_field,
	    raw.boolean_array_field
	FROM
	    raw
	LEFT JOIN
	    grouped_unnest_struct_array_field joined_struct_array_field ON raw.rowid = joined_struct_array_field.rowid;
	*/

	/* 	WITH raw AS (

	 */
	str.WriteString("WITH raw AS (\n")
	str.WriteString(helpers.Tabify(q, "\t"))
	str.WriteString("\n)")

	// for every struct[] field, we need to add various CTEs
	for _, structSliceCol := range structSliceColumns {

		/*
			, unnest_struct_array_field AS (
			    SELECT
			        rowid,
		*/
		unnestName := fmt.Sprintf(`unnest_%s`, structSliceCol.ColumnName)

		str.WriteString(fmt.Sprintf(`, %s AS (
    SELECT
        rowid,`, unnestName))

		/*
			UNNEST(COALESCE("struct_array_field", ARRAY[]::STRUCT("StructStringField" VARCHAR, "StructIntField" INTEGER)[])::STRUCT("StructStringField" VARCHAR, "StructIntField" INTEGER)[]) AS struct_array_field
		*/

		str.WriteString(fmt.Sprintf(`
		UNNEST(COALESCE("%s", ARRAY[]::%s)::%s) AS %s`, structSliceCol.ColumnName, structSliceCol.FullType(), structSliceCol.FullType(), structSliceCol.ColumnName))

		/*
		   	FROM
		   		raw
		   )
		*/

		str.WriteString(`
	FROM
		raw
)`)

		/*
		   , rebuild_unnest_struct_array_field AS (
		      SELECT
		   	   rowid,
		*/
		rebuildName := fmt.Sprintf(`rebuild_%s`, unnestName)

		str.WriteString(fmt.Sprintf(`, %s AS (
	SELECT
		rowid`, rebuildName))

		// loop over fields in the struct
		/*
			        struct_array_field->>'StructStringField' AS StructArrayField_StructStringField,
			)
		*/
		for _, structField := range structSliceCol.StructFields {
			str.WriteString(",\n")
			str.WriteString(fmt.Sprintf(`		%s->>'%s' AS %s_%s`, structSliceCol.ColumnName, structField.SourceName, structSliceCol.SourceName, structField.SourceName))
		}
		/*
		   	FROM
		   		unnest_struct_array_field
		   )
		*/

		str.WriteString(fmt.Sprintf(`
	FROM
		%s
)`, unnestName))

		/*
		      , grouped_unnest_struct_array_field AS (
		      	    SELECT
		      	        rowid,
		      	        array_agg(struct_pack(
		   				struct_string_field := StructArrayField_StructStringField::VARCHAR,
		      	            struct_int_field := StructArrayField_StructIntField::INTEGER
		      	        )) AS struct_array_field
		      	    FROM
		      	        rebuild_unnest_struct_array_field
		      	    GROUP BY
		      	        rowid
		      	)

		*/
		groupedName := fmt.Sprintf(`grouped_%s`, unnestName)
		str.WriteString(fmt.Sprintf(`, %s AS (`, groupedName))
		str.WriteString(`
	SELECT
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
		)) AS %s	
	FROM
		%s	
	GROUP BY
		rowid	
)`, structSliceCol.ColumnName, rebuildName))

	}
	// build the final select
	/*
		SELECT
			    COALESCE(joined_struct_array_field.struct_array_field, NULL) AS struct_array_field,
			    raw.int_field,
			    raw.string_field,
			    raw.float_field,
			    raw.boolean_field,
			    raw.int_array_field,
			    raw.string_array_field,
			    raw.float_array_field,
			    raw.boolean_array_field
			FROM
			    raw
			LEFT JOIN
			    grouped_unnest_struct_array_field joined_struct_array_field ON raw.rowid = joined_struct_array_field.rowid;
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

		coalesceFields.WriteString(fmt.Sprintf(`	COALESCE(%s.%s, NULL) AS %s`, joinedName, column.ColumnName, column.ColumnName))
		leftJoins.WriteString(fmt.Sprintf(`LEFT JOIN
	%s %s ON raw.rowid = %s.rowid`, groupedName, joinedName, joinedName))

	}

	// now construct final select
	str.WriteString(fmt.Sprintf(`
SELECT
%s`, coalesceFields.String()))

	for _, column := range rowSchema.Columns {
		if column.Type != "STRUCT[]" {
			str.WriteString(",\n")
			str.WriteString(fmt.Sprintf(`	raw.%s`, column.ColumnName))
		}
	}
	str.WriteString(fmt.Sprintf(`
FROM
	raw	
%s`, leftJoins.String()))

	return str.String()
}

// Return the SQL line to select the given field
func getSqlForField(column *schema.ColumnSchema, tabs int) string {
	// Calculate the tab spacing
	tab := strings.Repeat("\t", tabs)

	switch column.Type {
	case "STRUCT":
		var str strings.Builder

		// Start CASE logic to handle NULL values for the struct
		str.WriteString(fmt.Sprintf("%sCASE\n", tab))
		str.WriteString(fmt.Sprintf("%s\tWHEN \"%s\" IS NULL THEN NULL\n", tab, column.SourceName))
		str.WriteString(fmt.Sprintf("%s\tELSE struct_pack(\n", tab))

		// Add nested fields to the struct_pack
		for j, nestedColumn := range column.StructFields {
			if j > 0 {
				str.WriteString(",\n")
			}
			parentName := fmt.Sprintf("\"%s\"", column.SourceName)
			str.WriteString(getTypeSqlForStructField(nestedColumn, parentName, tabs+2))
		}

		// Close struct_pack and CASE
		str.WriteString(fmt.Sprintf("\n%s\t)\n", tab))
		str.WriteString(fmt.Sprintf("%sEND AS \"%s\"", tab, column.ColumnName))
		return str.String()

	case "JSON":
		// Convert the value using json()
		return fmt.Sprintf("%sjson(\"%s\") AS \"%s\"", tab, column.SourceName, column.ColumnName)

	default:
		// Scalar fields
		return fmt.Sprintf("%s\"%s\" AS \"%s\"", tab, column.SourceName, column.ColumnName)
	}
}

// Return the SQL line to pack the given field as a struct
func getTypeSqlForStructField(column *schema.ColumnSchema, parentName string, tabs int) string {
	tab := strings.Repeat("\t", tabs)

	switch column.Type {
	case "STRUCT":
		var str strings.Builder

		// Add CASE logic to handle NULL values for the struct
		str.WriteString(fmt.Sprintf("%s\"%s\" := CASE\n", tab, column.ColumnName))
		str.WriteString(fmt.Sprintf("%s\tWHEN %s.\"%s\" IS NULL THEN NULL\n", tab, parentName, column.SourceName))
		str.WriteString(fmt.Sprintf("%s\tELSE struct_pack(\n", tab))

		// Loop through nested fields and add them to the struct_pack
		for j, nestedColumn := range column.StructFields {
			if j > 0 {
				str.WriteString(",\n")
			}
			// Use the current field as the new parent for recursion
			newParent := fmt.Sprintf("%s.\"%s\"", parentName, column.SourceName)
			str.WriteString(getTypeSqlForStructField(nestedColumn, newParent, tabs+2))
		}

		// Close struct_pack and CASE
		str.WriteString(fmt.Sprintf("\n%s\t)\n", tab))
		str.WriteString(fmt.Sprintf("%sEND", tab))
		return str.String()

	default:
		// Scalar fields
		return fmt.Sprintf("%s\"%s\" := %s.\"%s\"::%s", tab, column.ColumnName, parentName, column.SourceName, column.Type)
	}
}
