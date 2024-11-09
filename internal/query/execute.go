package query

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/spf13/viper"
	"github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/tailpipe/internal/filepaths"
)

func ExecuteQuery(ctx context.Context, query string) error {
	// Open a DuckDB connection
	db, err := sql.Open("duckdb", filepaths.TailpipeDbFilePath())
	if err != nil {
		return err
	}

	defer db.Close()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}

	switch viper.GetString(constants.ArgOutput) {
	case constants.OutputFormatJSON:
		panic("not implemented")
	case constants.OutputFormatCSV:
		panic("not implemented")
	case constants.OutputFormatTable:
		return DisplayResultTable(ctx, rows)
	default:
		return fmt.Errorf("unknown output format: %s", viper.GetString(constants.ArgOutput))
	}
}
