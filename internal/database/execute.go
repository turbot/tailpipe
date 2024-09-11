package database

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/filepaths"
)

func AddTableView(ctx context.Context, tableName string) error {
	dataDir := config.GlobalWorkspaceProfile.GetDataDir()

	query := fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s AS SELECT * FROM '%s/%s/*/*/*/*.parquet'", tableName, dataDir, tableName)

	_, err := Execute(ctx, query)
	return err
}

// Execute opens then workspace databasew, executes a query and closes the db again
func Execute(ctx context.Context, query string) (sql.Result, error) {
	// Open a DuckDB connection
	db, err := sql.Open("duckdb", filepaths.TailpipeDbFilePath())
	if err != nil {
		return nil, err
	}

	defer db.Close()

	// execute query
	return db.ExecContext(ctx, query)
}

// Execute opens then workspace databasew, executes a query and closes the db again
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
