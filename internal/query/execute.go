package query

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/turbot/pipe-fittings/querydisplay"
	"github.com/turbot/pipe-fittings/queryresult"
	"github.com/turbot/tailpipe/internal/filepaths"
)

func ExecuteQuery(ctx context.Context, query string) (int, error) {
	// Open a DuckDB connection
	db, err := sql.Open("duckdb", filepaths.TailpipeDbFilePath())
	if err != nil {
		return 0, err
	}

	defer db.Close()

	// Run the query
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}

	// Execute the query
	result, err := Execute(ctx, rows, query)
	if err != nil {
		return 0, err
	}

	// show output
	_, rowErrors := querydisplay.ShowOutput(ctx, result)
	if rowErrors > 0 {
		// TODO find a way to return the error
		return rowErrors, fmt.Errorf("Error: query execution failed")
	}
	return 0, nil
}

type TimingMetadata struct {
	Duration time.Duration
}

func (t TimingMetadata) GetTiming() any {
	return t
}

func Execute(ctx context.Context, rows *sql.Rows, query string) (res *queryresult.Result[TimingMetadata], err error) {

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
	for rows.Next() {
		// Create a slice to hold the values for each row
		columnsData := make([]interface{}, len(colDefs))
		columnPointers := make([]interface{}, len(colDefs))

		// Fill columnPointers with pointers to each item in columnsData
		for i := range columnsData {
			columnPointers[i] = &columnsData[i]
		}

		// Scan the current row into columnPointers
		if err := rows.Scan(columnPointers...); err != nil { //nolint:staticcheck // TODO handle the error
			// return result, err
		}

		result.StreamRow(columnsData)
	}
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
