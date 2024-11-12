package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/thediveo/enumflag/v2"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/interactive"
	"github.com/turbot/tailpipe/internal/query"
	"strings"
)

// variable used to assign the output mode flag
var queryOutputMode = constants.QueryOutputModeTable

func queryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "query [sql]",
		Args:             cobra.MaximumNArgs(1),
		TraverseChildren: true,
		Run:              runQueryCmd,
		Short:            "execute a query against the workspace database",
		Long:             `execute a query against the workspace database.`,
	}

	cmdconfig.OnCmd(cmd).
		AddVarFlag(enumflag.New(&queryOutputMode, pconstants.ArgOutput, constants.QueryOutputModeIds, enumflag.EnumCaseInsensitive),
			pconstants.ArgOutput,
			fmt.Sprintf("Output format; one of: %s", strings.Join(constants.FlagValues(constants.QueryOutputModeIds), ", ")))

	return cmd
}

func runQueryCmd(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	var err error
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
			error_helpers.ShowError(ctx, err)
		}
		setExitCodeForQueryError(err)
	}()

	// if an arg was passed, just execute the query
	if len(args) == 0 {
		err = interactive.RunInteractiveQuery(ctx)
	} else {
		err = query.ExecuteQuery(ctx, args[0])
	}
	error_helpers.FailOnError(err)

}

func setExitCodeForQueryError(err error) {
	// if exit code already set, leave as is
	if exitCode != 0 || err == nil {
		return
	}

	// TODO #errors - assign exit codes  https://github.com/turbot/tailpipe/issues/35
	exitCode = 1
}
