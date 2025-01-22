package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/thediveo/enumflag/v2"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/interactive"
	"github.com/turbot/tailpipe/internal/query"
)

// variable used to assign the output mode flag
var queryOutputMode = constants.QueryOutputModeTable

func queryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "query [sql] [flags]",
		Args:             cobra.MaximumNArgs(1),
		TraverseChildren: true,
		Run:              runQueryCmd,
		Short:            "Execute a query against the workspace database",
		Long: `Execute SQL queries interactively, or by a query argument.

To open the interactive query shell, run tailpipe query with no arguments. The query shell 
provides a way to explore your data and run multiple queries.

If a query string is passed on the command line then it will be run immediately and the command 
will exit. Alternatively, you may specify one or more files containing SQL statements. You can 
run multiple SQL files by passing a glob or a space-separated list of file names.`,
	}

	// args `from` and `to` accept:
	// - ISO 8601 date (2024-01-01)
	// - ISO 8601 datetime (2006-01-02T15:04:05)
	// - ISO 8601 datetime with ms (2006-01-02T15:04:05.000)
	// - RFC 3339 datetime with timezone (2006-01-02T15:04:05Z07:00)
	// - relative time formats (T-2Y, T-10m, T-10W, T-180d, T-9H, T-10M)

	cmdconfig.OnCmd(cmd).
		AddVarFlag(enumflag.New(&queryOutputMode, pconstants.ArgOutput, constants.QueryOutputModeIds, enumflag.EnumCaseInsensitive),
			pconstants.ArgOutput,
			fmt.Sprintf("Output format; one of: %s", strings.Join(constants.FlagValues(constants.QueryOutputModeIds), ", "))).
		AddStringFlag(pconstants.ArgFrom, "", "Specify the start time").
		AddStringFlag(pconstants.ArgTo, "", "Specify the end time").
		AddBoolFlag(pconstants.ArgHeader, true, "Include column headers csv and table output").
		AddStringFlag(pconstants.ArgSeparator, ",", "Separator string for csv output")

	return cmd
}

func runQueryCmd(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	var err error
	var failures int

	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
		if err != nil {
			error_helpers.ShowError(ctx, err)
			setExitCodeForQueryError(err)
		}
	}()

	// get a connection to the database
	var db *sql.DB
	db, err = openDatabaseConnection(ctx)
	if err != nil {
		return
	}
	defer db.Close()

	// if an arg was passed, just execute the query
	if len(args) == 0 {
		err = interactive.RunInteractivePrompt(ctx, db)
	} else {
		failures, err = query.ExecuteQuery(ctx, args[0], db)
	}
	if failures > 0 {
		exitCode = pconstants.ExitCodeQueryExecutionFailed
		error_helpers.FailOnError(err)
	}

}

// generate a db file - this will respect any time/index filters specified in the command args
func openDatabaseConnection(ctx context.Context) (*sql.DB, error) {
	dbFilePath, err := generateDbFile(ctx)
	if err != nil {
		return nil, err
	}
	// Open a DuckDB connection
	return sql.Open("duckdb", dbFilePath)
}

func setExitCodeForQueryError(err error) {
	// if exit code already set, leave as is
	if exitCode != 0 || err == nil {
		return
	}

	// TODO #errors - assign exit codes  https://github.com/turbot/tailpipe/issues/106
	exitCode = 1
}
