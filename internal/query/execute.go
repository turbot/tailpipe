package query

import (
	"context"
	"database/sql"
	"encoding/json"
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
	// Run the query
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		// if this error is due to trying to select a table which exists in partition config,
		// but there is no view defined (as no rows have been collected), return a special error
		err := handleMissingViewError(err)

		return 0, err
	}

	// Execute the query
	result, err := Execute(ctx, rows)
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

func Execute(ctx context.Context, rows *sql.Rows) (res *queryresult.Result[TimingMetadata], err error) {

	colDefs, err := fetchColumnDefs(rows)
	if err != nil {
		return nil, err
	}

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

		// Post-process JSON columns to convert them back to JSON strings
		for i, colDef := range colDefs {
			// Check for JSON columns using multiple criteria
			dataType := strings.ToLower(colDef.DataType)

			// Primary detection: Check if DuckDB reports this as a JSON type
			isJSONByType := dataType == "json" || strings.Contains(dataType, "json")

			// Secondary detection: Check if the data looks like JSON data structures
			// This handles cases where DuckDB might not explicitly mark it as JSON type
			isJSONByData := false
			if columnsData[i] != nil {
				dataTypeStr := fmt.Sprintf("%T", columnsData[i])
				// Check for various Go types that could represent JSON data
				isJSONByData = (strings.Contains(dataTypeStr, "map[") ||
					strings.Contains(dataTypeStr, "[]interface") ||
					strings.HasPrefix(dataTypeStr, "[]map"))
			}

			if (isJSONByType || isJSONByData) && columnsData[i] != nil {
				// Convert the scanned data back to JSON string for proper display
				if jsonBytes, err := json.Marshal(columnsData[i]); err == nil {
					columnsData[i] = string(jsonBytes)
				}
			}
		}

		result.StreamRow(columnsData)
		rowCount++

		statushooks.SetStatus(ctx, fmt.Sprintf("Loading results: %3s", utils.HumanizeNumber(rowCount)))
	}
	statushooks.Done(ctx)
}

// FetchColumnDefs extracts column definitions from sql.Rows and returns a slice of ColumnDef.
func fetchColumnDefs(rows *sql.Rows) ([]*queryresult.ColumnDef, error) {
	// Get column names
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Get column types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// Initialize a slice to hold column definitions
	var columnDefs []*queryresult.ColumnDef

	for i, colType := range columnTypes {
		columnDef := &queryresult.ColumnDef{
			Name:         columnNames[i],
			DataType:     colType.DatabaseTypeName(),
			OriginalName: columnNames[i], // Set this if you have a way to obtain the original name (optional) - this would be needed when multiple same columns are requested
		}

		// Append to the list of column definitions
		columnDefs = append(columnDefs, columnDef)
	}

	return columnDefs, nil
}
