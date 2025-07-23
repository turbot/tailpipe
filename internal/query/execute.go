package query

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/pipe-fittings/v2/query"
	"github.com/turbot/pipe-fittings/v2/querydisplay"
	"github.com/turbot/pipe-fittings/v2/queryresult"
	"github.com/turbot/pipe-fittings/v2/statushooks"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
)

func RunBatchSession(ctx context.Context, args []string, db *database.DuckDb) (int, []error) {

	var totalFailedRows int
	var errors []error

	// get the queries from the args
	queries, err := query.GetQueriesFromArgs(args)
	error_helpers.FailOnErrorWithMessage(err, "failed to get queries from args")

	for i, arg := range queries {
		// increment failures if the query fails
		failures, err := ExecuteQuery(ctx, arg, db)
		// accumulate the failures
		totalFailedRows += failures
		if err != nil {
			error_helpers.ShowWarning(fmt.Sprintf("query %d of %d failed: %v", i+1, len(args), err))
			errors = append(errors, err)
		}
	}

	return totalFailedRows, errors
}

func ExecuteQuery(ctx context.Context, query string, db *database.DuckDb) (int, error) {
	// Get column definitions first
	colDefs, err := ExecuteDescribeQuery(query, db)
	if err != nil {
		// Handle missing view errors from DESCRIBE query as well
		err := handleMissingViewError(err)
		return 0, err
	}

	// Run the query
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		// if this error is due to trying to select a table which exists in partition config,
		// but there is no view defined (as no rows have been collected), return a special error
		err := handleMissingViewError(err)

		return 0, err
	}

	// Execute the query
	result, err := Execute(ctx, rows, colDefs)
	if err != nil {
		return 0, err
	}

	// show output
	_, rowErrors := querydisplay.ShowOutput(ctx, result)
	if rowErrors > 0 {
		// TODO #errors find a way to return the error
		return rowErrors, fmt.Errorf("query execution failed")
	}
	return 0, nil
}

// ExecuteDescribeQuery executes a DESCRIBE query to get column definitions
func ExecuteDescribeQuery(query string, db *database.DuckDb) ([]*queryresult.ColumnDef, error) {
	// Remove trailing semicolon from query to avoid DESCRIBE syntax errors
	cleanQuery := strings.TrimSpace(query)
	cleanQuery = strings.TrimSuffix(cleanQuery, ";")

	// Create DESCRIBE query
	describeQuery := fmt.Sprintf("DESCRIBE (%s)", cleanQuery)

	// Execute the describe query
	rows, err := db.Query(describeQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Initialize a slice to hold column definitions
	var columnDefs []*queryresult.ColumnDef

	// Process the DESCRIBE results
	for rows.Next() {
		var columnName, columnType string
		var nullable, key, defaultValue, extra sql.NullString

		// DESCRIBE returns: column_name, column_type, null, key, default, extra
		err := rows.Scan(&columnName, &columnType, &nullable, &key, &defaultValue, &extra)
		if err != nil {
			return nil, err
		}

		columnDef := &queryresult.ColumnDef{
			Name:         columnName,
			DataType:     columnType,
			OriginalName: columnName,
		}

		columnDefs = append(columnDefs, columnDef)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columnDefs, nil
}

func handleMissingViewError(err error) error {
	errorMessage := err.Error()
	// Define the regex to match the table name
	regex := regexp.MustCompile(`Catalog Error: Table with name ([a-zA-Z0-9_]+) does not exist`)

	// Find the matches
	matches := regex.FindStringSubmatch(errorMessage)
	if len(matches) > 1 {
		tableName := strings.ToLower(matches[1])
		// do we have a partition defined for this table
		for _, partition := range config.GlobalConfig.Partitions {
			if strings.ToLower(partition.TableName) == tableName {
				return fmt.Errorf("no data has been collected for table %s", tableName)
			}
		}
	}

	// return the original error
	return err
}

type TimingMetadata struct {
	Duration time.Duration
}

func (t TimingMetadata) GetTiming() any {
	return t
}

func Execute(ctx context.Context, rows *sql.Rows, colDefs []*queryresult.ColumnDef) (res *queryresult.Result[TimingMetadata], err error) {

	result := queryresult.NewResult[TimingMetadata](colDefs, TimingMetadata{})

	// stream rows from the query result
	go streamResults(ctx, rows, result, colDefs)

	return result, nil
}

func streamResults(ctx context.Context, rows *sql.Rows, result *queryresult.Result[TimingMetadata], colDefs []*queryresult.ColumnDef) {
	defer func() {
		rows.Close()
		if err := rows.Err(); err != nil {
			result.StreamError(err)
		}
		// close the channels in the result object
		result.Close()

	}()
	statushooks.Show(ctx)

	rowCount := 0
	for rows.Next() {
		if ctx.Err() != nil {
			return
		}
		// Create a slice to hold the values for each row
		columnsData := make([]interface{}, len(colDefs))
		columnPointers := make([]interface{}, len(colDefs))

		// Fill columnPointers with pointers to each item in columnsData
		for i := range columnsData {
			columnPointers[i] = &columnsData[i]
		}

		// Scan the current row into columnPointers
		if err := rows.Scan(columnPointers...); err != nil {
			slog.Warn("Error scanning row", "error", err)
			return
		}

		result.StreamRow(columnsData)
		rowCount++

		statushooks.SetStatus(ctx, fmt.Sprintf("Loading results: %3s", utils.HumanizeNumber(rowCount)))
	}
	statushooks.Done(ctx)
}
