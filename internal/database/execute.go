package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/filepaths"
)

func AddTableView(ctx context.Context, tableName string, db *sql.DB, filters ...string) error {
	dataDir := config.GlobalWorkspaceProfile.GetDataDir()

	filterString := ""
	if len(filters) > 0 {
		// join filters
		filterString = fmt.Sprintf(" WHERE %s", strings.Join(filters, " AND "))
	}

	// TODO wrap path into function?
	query := fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s AS SELECT * FROM '%s/%s/*/*/*/*.parquet'%s", tableName, dataDir, tableName, filterString)

	_, err := Execute(ctx, query, db)
	return err
}

// Execute opens then workspace database, executes a query (with no return rows) and closes the db again
func Execute(ctx context.Context, query string, db *sql.DB) (sql.Result, error) {
	// execute query
	return db.ExecContext(ctx, query)
}

// Query opens then workspace database, executes a query and closes the db again
func Query(ctx context.Context, query string) (*sql.Rows, error) {
	// Open a DuckDB connection
	db, err := sql.Open("duckdb", filepaths.TailpipeDbFilePath())
	if err != nil {
		return nil, err
	}

	defer db.Close()

	// execute query
	return db.QueryContext(ctx, query)
}
