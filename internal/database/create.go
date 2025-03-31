package database

import (
	"context"
	_ "github.com/marcboeker/go-duckdb/v2"
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
	db, err := NewDuckDb(WithDbFile(databaseFilePath))
	if err != nil {
		return err
	}
	defer db.Close()

	return AddTableViews(ctx, db)
}
