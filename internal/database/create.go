package database

import (
	"context"
	"database/sql"
	_ "github.com/marcboeker/go-duckdb"
	filehelpers "github.com/turbot/go-kit/files"
	_ "github.com/turbot/go-kit/helpers"
	_ "github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe/internal/filepaths"
)

func EnsureDatabaseFile(ctx context.Context) error {
	databaseFilePath := filepaths.TailpipeDbFilePath()
	if filehelpers.FileExists(databaseFilePath) {
		return nil
	}

	//
	// Open a DuckDB connection (creates the file if it doesn't exist)
	db, err := sql.Open("duckdb", databaseFilePath)
	if err != nil {
		return err
	}
	defer db.Close()

	return AddTableViews(ctx, db)
}
