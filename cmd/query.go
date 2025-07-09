package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thediveo/enumflag/v2"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/interactive"
	"github.com/turbot/tailpipe/internal/query"
)

// variable used to assign the output mode flag
var queryOutputMode = constants.QueryOutputModeTable

func queryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "query [sql] [flags]",
		Args:             cobra.ArbitraryArgs,
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
		AddStringSliceFlag(pconstants.ArgIndex, nil, "Specify the index to use").
		AddStringSliceFlag(pconstants.ArgPartition, nil, "Specify the partition to use").
		AddBoolFlag(pconstants.ArgHeader, true, "Include column headers csv and table output").
		AddStringFlag(pconstants.ArgSeparator, ",", "Separator string for csv output")

	return cmd
}

func runQueryCmd(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	var err error
	var failures int
	var errs []error

	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
		if err != nil {
			error_helpers.ShowError(ctx, err)
			setExitCodeForQueryError(err)
		}
	}()

	// get a connection to the database, with DuckLake enabled
	db, err := database.NewDuckDb(database.WithDuckLakeEnabled(true))
	if err != nil {
		return
	}
	defer db.Close()

	// if an arg was passed, just execute the query
	if len(args) == 0 {
		// set the interactive flag - this is used by the query display code to decide whether to page the results
		viper.Set(pconstants.ConfigKeyInteractive, true)

		err = interactive.RunInteractivePrompt(ctx, db)
		error_helpers.FailOnError(err)
	} else {
		failures, errs = query.RunBatchSession(ctx, args, db)
	}
	// if there were any row errors or any missing view errors, exit with a non-zero code
	if failures > 0 || len(errs) > 0 {
		// if there were any errors, they would have been shown already from `RunBatchSession` - just set the exit code
		exitCode = pconstants.ExitCodeQueryExecutionFailed
	}
}

func setExitCodeForQueryError(err error) {
	// if exit code already set, leave as is
	if exitCode != 0 || err == nil {
		return
	}

	// TODO #errors - assign exit codes  https://github.com/turbot/tailpipe/issues/106
	exitCode = 1
}
