package query

import (
	"context"
	"fmt"
	"github.com/spf13/viper"
	"github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/tailpipe/internal/database"
)

func ExecutQuery(ctx context.Context, sql string) error {
	rows, err := database.Query(ctx, sql)
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
