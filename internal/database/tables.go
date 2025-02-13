package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/filepaths"
)

// AddTableViews creates a view for each table in the data directory, applying the provided duck db filters to the view query
func AddTableViews(ctx context.Context, db *sql.DB, filters ...string) error {
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
func createAndDropEmptyView(ctx context.Context, db *sql.DB) {
	_ = AddTableView(ctx, "empty", db)
	// drop again
	_, _ = db.ExecContext(ctx, "DROP VIEW empty")
}

func AddTableView(ctx context.Context, tableName string, db *sql.DB, filters ...string) error {
	// TODO #SQL use params to avoid injection

	dataDir := config.GlobalWorkspaceProfile.GetDataDir()
	// Path to the Parquet directory
	// hive structure is <workspace>/tp_table=<table_name>/tp_partition=<partition>/tp_index=<index>/tp_date=<date>.parquet
	parquetPath := filepaths.GetParquetFileGlobForTable(dataDir, tableName, "")

	// Step 1: Query the first Parquet file to infer columns
	columns, err := getColumnNames(ctx, parquetPath, db)
	if err != nil {
		// if this is because no parquet files match, suppress the error
		if strings.Contains(err.Error(), "IO Error: No files found that match the pattern") {
			return nil
		}
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
		wrappedCol := fmt.Sprintf(`"%s"`, col)
		if overrideType, ok := typeOverrides[col]; ok {
			// Apply the override with casting
			selectClauses = append(selectClauses, fmt.Sprintf("CAST(%s AS %s) AS %s", col, overrideType, wrappedCol))
		} else {
			// Add the column as-is
			selectClauses = append(selectClauses, wrappedCol)
		}
	}
	selectClause := strings.Join(selectClauses, ", ")

	// Step 3: Build the WHERE clause
	filterString := ""
	if len(filters) > 0 {
		filterString = fmt.Sprintf(" WHERE %s", strings.Join(filters, " AND "))
	}

	// Step 4: Construct the final query
	query := fmt.Sprintf( //nolint: gosec // this is a controlled query
		"CREATE OR REPLACE VIEW %s AS SELECT %s FROM '%s'%s",
		tableName, selectClause, parquetPath, filterString,
	)

	// Execute the query
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create view: %w", err)
	}
	return nil
}

// query the provided parquet path to get the columns
func getColumnNames(ctx context.Context, parquetPath string, db *sql.DB) ([]string, error) {
	columnQuery := fmt.Sprintf("SELECT * FROM '%s' LIMIT 0", parquetPath) //nolint: gosec // this is a controlled query
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

	// Sort column names alphabetically but with tp_ fields on the end
	tpPrefix := "tp_"
	slices.SortFunc(columns, func(a, b string) int {
		isPrefixedA, isPrefixedB := strings.HasPrefix(a, tpPrefix), strings.HasPrefix(b, tpPrefix)
		switch {
		case isPrefixedA && !isPrefixedB:
			return 1 // a > b
		case !isPrefixedA && isPrefixedB:
			return -1 // a < b
		default:
			return strings.Compare(a, b) // standard alphabetical sort
		}
	})

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

func GetRowCount(ctx context.Context, tableName string, partitionName *string) (int64, error) {
	// Open a DuckDB connection
	db, err := sql.Open("duckdb", filepaths.TailpipeDbFilePath())
	if err != nil {
		return 0, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer db.Close()

	var tableNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	if !tableNameRegex.MatchString(tableName) {
		return 0, fmt.Errorf("invalid table name")
	}
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName) // #nosec G201 // this is a controlled query tableName must match a regex
	if partitionName != nil {
		query = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE tp_partition = '%s'", tableName, *partitionName) // #nosec G201 // this is a controlled query tableName must match a regex
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}
	defer rows.Close()

	var count int64
	if rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			return 0, fmt.Errorf("failed to scan row count: %w", err)
		}
	}
	return count, nil
}

func GetTableViews(ctx context.Context) ([]string, error) {
	// Open a DuckDB connection
	db, err := sql.Open("duckdb", filepaths.TailpipeDbFilePath())
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer db.Close()

	query := "SELECT table_name FROM information_schema.tables WHERE table_type='VIEW';"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get table views: %w", err)
	}
	defer rows.Close()

	var tableViews []string
	for rows.Next() {
		var tableView string
		err = rows.Scan(&tableView)
		if err != nil {
			return nil, fmt.Errorf("failed to scan table view: %w", err)
		}
		tableViews = append(tableViews, tableView)
	}
	return tableViews, nil
}

func GetTableViewSchema(ctx context.Context, viewName string) (map[string]string, error) {
	// Open a DuckDB connection
	db, err := sql.Open("duckdb", filepaths.TailpipeDbFilePath())
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer db.Close()

	query := `
		SELECT column_name, data_type 
		FROM information_schema.columns 
		WHERE table_name = ? ORDER BY columns.column_name;
	`
	rows, err := db.QueryContext(ctx, query, viewName)
	if err != nil {
		return nil, fmt.Errorf("failed to get view schema for %s: %w", viewName, err)
	}
	defer rows.Close()

	schema := make(map[string]string)
	for rows.Next() {
		var columnName, columnType string
		err = rows.Scan(&columnName, &columnType)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column schema: %w", err)
		}
		if strings.HasPrefix(columnType, "STRUCT") {
			columnType = "STRUCT"
		}
		schema[columnName] = columnType
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over view schema rows: %w", err)
	}

	return schema, nil
}
