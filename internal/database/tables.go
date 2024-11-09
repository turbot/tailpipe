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

// optimisation - it seems the first time DuckDB creates a view which inspects the file system it is slow
// creating and empty view first and then dropping it seems to speed up the process
func createAndDropEmptyView(ctx context.Context, db *sql.DB) {
	_ = AddTableView(ctx, "empty", db)
	// drop again
	_, _ = db.ExecContext(ctx, "DROP VIEW empty")
	return
}

func AddTableView(ctx context.Context, tableName string, db *sql.DB, filters ...string) error {
	dataDir := config.GlobalWorkspaceProfile.GetDataDir()

	filterString := ""
	if len(filters) > 0 {
		// join filters
		filterString = fmt.Sprintf(" WHERE %s", strings.Join(filters, " AND "))
	}

	// TODO wrap path into function?
	query := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM '%s/%s/*/*/*/*.parquet'%s", tableName, dataDir, tableName, filterString)

	_, err := db.ExecContext(ctx, query)
	return err
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
