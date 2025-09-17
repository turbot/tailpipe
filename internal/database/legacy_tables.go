package database

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/tailpipe-plugin-sdk/helpers"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/filepaths"
)

// AddTableViews creates a view for each table in the data directory, applying the provided duck db filters to the view query
func AddTableViews(ctx context.Context, db *DuckDb, filters ...string) error {
	tables, err := getDirNames(config.GlobalWorkspaceProfile.GetDataDir())
	if err != nil {
		return fmt.Errorf("failed to get tables: %w", err)
	}

	// optimisation - it seems the first time DuckDB creates a view which inspects the file system it is slow
	// creating and empty view first and then dropping it seems to speed up the process
	createAndDropEmptyView(ctx, db)

	//create a view for each table
	for _, tableFolder := range tables {
		// create a view for the table
		// the tab;le folder is a hive partition folder so will have the format tp_table=table_name
		table := strings.TrimPrefix(tableFolder, "tp_table=")
		err = AddTableView(ctx, table, db, filters...)
		if err != nil {
			return err
		}
	}
	return nil
}

// NOTE: tactical optimisation - it seems the first time DuckDB creates a view which inspects the file system it is slow
// creating and empty view first and then dropping it seems to speed up the process
func createAndDropEmptyView(ctx context.Context, db *DuckDb) {
	_ = AddTableView(ctx, "empty", db)
	// drop again
	_, _ = db.ExecContext(ctx, "DROP VIEW empty")
}

func AddTableView(ctx context.Context, tableName string, db *DuckDb, filters ...string) error {
	slog.Info("creating view", "table", tableName, "filters", filters)

	dataDir := config.GlobalWorkspaceProfile.GetDataDir()
	// Path to the Parquet directory
	// hive structure is <workspace>/tp_table=<table_name>/tp_partition=<partition>/tp_index=<index>/tp_date=<date>.parquet
	parquetPath := filepaths.GetParquetFileGlobForTable(dataDir, tableName, "")

	// Step 1: Query the first Parquet file to infer columns
	columns, err := getColumnNames(ctx, parquetPath, db)
	if err != nil {
		// if this is because no parquet files match, suppress the error
		if strings.Contains(err.Error(), "IO Error: No files found that match the pattern") || error_helpers.IsCancelledError(err) {
			return nil
		}
		return err
	}

	// Step 2: Build the select clause - cast tp_index as string
	// (this is necessary as duckdb infers the type from the partition column name
	// if the index looks like a number, it will infer the column as an int)
	var typeOverrides = map[string]string{
		"tp_partition": "varchar",
		"tp_index":     "varchar",
		"tp_date":      "date",
	}
	var selectClauses []string
	for _, col := range columns {
		wrappedCol := fmt.Sprintf(`"%s"`, col)
		if overrideType, ok := typeOverrides[col]; ok {
			// Apply the override with casting
			selectClauses = append(selectClauses, fmt.Sprintf("cast(%s as %s) as %s", col, overrideType, wrappedCol))
		} else {
			// Add the column as-is
			selectClauses = append(selectClauses, wrappedCol)
		}
	}
	selectClause := strings.Join(selectClauses, ", ")

	// Step 3: Build the where clause
	filterString := ""
	if len(filters) > 0 {
		filterString = fmt.Sprintf(" where %s", strings.Join(filters, " and "))
	}

	// Step 4: Construct the final query
	query := fmt.Sprintf(
		"create or replace view %s as select %s from '%s'%s",
		tableName, selectClause, parquetPath, filterString,
	)

	// Execute the query
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		slog.Warn("failed to create view", "table", tableName, "error", err)
		return fmt.Errorf("failed to create view: %w", err)
	}
	slog.Info("created view", "table", tableName)
	return nil
}

// query the provided parquet path to get the columns
func getColumnNames(ctx context.Context, parquetPath string, db *DuckDb) ([]string, error) {
	columnQuery := fmt.Sprintf("select * from '%s' limit 0", parquetPath)
	rows, err := db.QueryContext(ctx, columnQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Retrieve column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Sort column names alphabetically but with tp_ fields on the end
	columns = helpers.SortColumnsAlphabetically(columns)

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
