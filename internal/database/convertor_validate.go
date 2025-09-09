package database

import (
	"fmt"
	"strings"
)

// validateRows copies the data from the given select query to a temp table and validates required fields are non null
// it also validates that the schema of the chunk is the same as the inferred schema and if it is not, reports a useful error
// the query count of invalid rows and a list of null fields
//
//nolint:unused // TODO re-add validation https://github.com/turbot/tailpipe/issues/479
func (w *Converter) validateRows(jsonlFilePaths []string) error {
	// build array of required columns to validate
	var requiredColumns []string
	for _, col := range w.conversionSchema.Columns {
		if col.Required {
			// if the column is required, add it to the list of columns to validate
			requiredColumns = append(requiredColumns, col.ColumnName)
		}
	}

	// if we have no columns to validate, biuld a  validation query to return the number of invalid rows and the columns with nulls
	validationQuery := w.buildValidationQuery(requiredColumns)

	row := w.db.QueryRow(validationQuery)
	var failedRowCount int64
	var columnsWithNullsInterface []interface{}

	err := row.Scan(&failedRowCount, &columnsWithNullsInterface)
	if err != nil {
		return w.handleSchemaChangeError(err, jsonlFilePaths[0])
	}

	if failedRowCount == 0 {
		// no rows with nulls - we are done
		return nil
	}

	// delete invalid rows from the temp table
	if err := w.deleteInvalidRows(requiredColumns); err != nil {
		// failed to delete invalid rows - return an error
		err := handleConversionError(err, jsonlFilePaths...)
		return err
	}

	// Convert the interface slice to string slice
	var columnsWithNulls []string
	for _, col := range columnsWithNullsInterface {
		if col != nil {
			columnsWithNulls = append(columnsWithNulls, col.(string))
		}
	}

	// we have a failure - return an error with details about which columns had nulls
	return NewConversionError(NewRowValidationError(failedRowCount, columnsWithNulls), failedRowCount, jsonlFilePaths...)
}

// buildValidationQuery builds a query to copy the data from the select query to a temp table
// it then validates that the required columns are not null, removing invalid rows and returning
// the count of invalid rows and the columns with nulls
//
//nolint:unused // TODO re-add validation https://github.com/turbot/tailpipe/issues/479
func (w *Converter) buildValidationQuery(requiredColumns []string) string {
	var queryBuilder strings.Builder

	// Build the validation query that:
	// - Counts distinct rows that have null values in required columns
	// - Lists all required columns that contain null values
	queryBuilder.WriteString(`select
    count(distinct rowid) as rows_with_required_nulls,  -- Count unique rows with nulls in required columns
    coalesce(list(distinct col), []) as required_columns_with_nulls  -- List required columns that have null values, defaulting to empty list if NULL
from (`)

	// Step 3: For each required column we need to validate:
	// - Create a query that selects rows where this column is null
	// - Include the column name so we know which column had the null
	// - UNION ALL combines all these results (faster than UNION as we don't need to deduplicate)
	for i, col := range requiredColumns {
		if i > 0 {
			queryBuilder.WriteString("    union all\n")
		}
		// For each required column, create a query that:
		// - Selects the rowid (to count distinct rows)
		// - Includes the column name (to list which columns had nulls)
		// - Only includes rows where this column is null
		queryBuilder.WriteString(fmt.Sprintf("    select rowid, '%s' as col from temp_data where %s is null\n", col, col))
	}

	queryBuilder.WriteString(");")

	return queryBuilder.String()
}

// buildNullCheckQuery builds a WHERE clause to check for null values in the specified columns
//
//nolint:unused // TODO re-add validation https://github.com/turbot/tailpipe/issues/479
func (w *Converter) buildNullCheckQuery(requiredColumns []string) string {

	// build a slice of null check conditions
	conditions := make([]string, len(requiredColumns))
	for i, col := range requiredColumns {
		conditions[i] = fmt.Sprintf("%s is null", col)
	}
	return strings.Join(conditions, " or ")
}

// deleteInvalidRows removes rows with null values in the specified columns from the temp table
//
//nolint:unused // TODO re-add validation https://github.com/turbot/tailpipe/issues/479
func (w *Converter) deleteInvalidRows(requiredColumns []string) error {
	whereClause := w.buildNullCheckQuery(requiredColumns)
	query := fmt.Sprintf("delete from temp_data where %s;", whereClause)

	_, err := w.db.Exec(query)
	return err
}
