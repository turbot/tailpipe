package query

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/turbot/pipe-fittings/querydisplay"
	"github.com/turbot/pipe-fittings/queryresult"
	"github.com/turbot/pipe-fittings/statushooks"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe/internal/config"
)

func ExecuteQuery(ctx context.Context, query string, db *sql.DB) (int, error) {
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
