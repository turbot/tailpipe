package database

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/turbot/tailpipe/internal/config"
	"os"
	"strings"
)

// AddTableViews creates a view for each table in the data directory, applying the provided duck db filters to the view query
func AddTableViews(ctx context.Context, db *sql.DB, filters ...string) error {
	tables, err := getDirNames(config.GlobalWorkspaceProfile.GetDataDir())
	if err != nil {
		return err
	}

	// optimisation - it seems the first time DuckDB creates a view which inspects the file system it is slow
	// creating and empty view first and then dropping it seems to speed up the process
	createAndDropEmptyView(ctx, db)

	//create a view for each table
	for _, table := range tables {
		// create a view for the table
		err = AddTableView(ctx, table, db, filters...)
		if err != nil {
			return err
		}
	}
	return nil
}

// NOTE: tactical optimisation - it seems the first time DuckDB creates a view which inspects the file system it is slow
// creating and empty view first and then dropping it seems to speed up the process
func createAndDropEmptyView(ctx context.Context, db *sql.DB) {
	_ = AddTableView(ctx, "empty", db)
	// drop again
	_, _ = db.ExecContext(ctx, "DROP VIEW empty")
}

func AddTableView(ctx context.Context, tableName string, db *sql.DB, filters ...string) error {
	dataDir := config.GlobalWorkspaceProfile.GetDataDir()
	// Path to the Parquet directory
	parquetPath := fmt.Sprintf("'%s/%s/*/*/*/*/*.parquet'", dataDir, tableName)

	// Step 1: Query the first Parquet file to infer columns
	columns, err := getColumnNames(ctx, parquetPath, db)
	if err != nil {
		return err
	}

	// Step 2: Build the SELECT clause - cast tp_index as string
	// (this is necessary as duckdb infers the type from the partition column name
	// if the index looks like a number, it will infer the column as an int)
	var typeOverrides = map[string]string{
		"tp_partition": "VARCHAR",
		"tp_index":     "VARCHAR",
		"tp_date":      "DATE",
	}
	var selectClauses []string
	for _, col := range columns {
		if overrideType, ok := typeOverrides[col]; ok {
			// Apply the override with casting
			selectClauses = append(selectClauses, fmt.Sprintf("CAST(%s AS %s) AS %s", col, overrideType, col))
		} else {
			// Add the column as-is
			selectClauses = append(selectClauses, col)
		}
	}
	selectClause := strings.Join(selectClauses, ", ")

	// Step 3: Build the WHERE clause
	filterString := ""
	if len(filters) > 0 {
		filterString = fmt.Sprintf(" WHERE %s", strings.Join(filters, " AND "))
	}

	// Step 4: Construct the final query
	query := fmt.Sprintf(
		"CREATE OR REPLACE VIEW %s AS SELECT %s FROM %s%s",
		tableName, selectClause, parquetPath, filterString,
	)

	// Execute the query
	_, err = db.ExecContext(ctx, query)
	return err
}

// query the provided parquet path to get the columns
func getColumnNames(ctx context.Context, parquetPath string, db *sql.DB) ([]string, error) {
	columnQuery := fmt.Sprintf("SELECT * FROM %s LIMIT 0", parquetPath)
	rows, err := db.QueryContext(ctx, columnQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to infer schema: %w", err)
	}
	defer rows.Close()

	// Retrieve column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %w", err)
	}
	return columns, nil
}

func getDirNames(folderPath string) ([]string, error) {
	var dirNames []string

	// Read the directory contents
	files, err := os.ReadDir(folderPath)
	if err != nil {
		return nil, err
	}

	// Loop through the contents and add directories to dirNames
	for _, file := range files {
		if file.IsDir() {
			dirNames = append(dirNames, file.Name())
		}
	}

	return dirNames, nil
}
