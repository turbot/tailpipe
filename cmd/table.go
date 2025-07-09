package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/thediveo/enumflag/v2"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/contexthelpers"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/pipe-fittings/v2/printers"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/display"
)

func tableCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "table [command]",
		Args:  cobra.NoArgs,
		Short: "List and show Tailpipe tables",
		Long: `Tailpipe table commands.

Tables define the structure of the data that is collected by Tailpipe.

Examples:

    # List all tables
    tailpipe table list

    # Show details for a specific table
    tailpipe table show aws_cloudtrail_log`,
	}

	cmd.AddCommand(tableListCmd())
	cmd.AddCommand(tableShowCmd())

	cmd.Flags().BoolP(pconstants.ArgHelp, "h", false, "Help for table")

	return cmd
}

// List Tables
func tableListCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "list",
		Args:  cobra.NoArgs,
		Run:   runTableListCmd,
		Short: "List all tables.",
		Long:  `List all tables.`,
	}

	cmdconfig.
		OnCmd(cmd).
		AddVarFlag(enumflag.New(&pluginOutputMode, pconstants.ArgOutput, constants.PluginOutputModeIds, enumflag.EnumCaseInsensitive),
			pconstants.ArgOutput,
			fmt.Sprintf("Output format; one of: %s", strings.Join(constants.FlagValues(constants.PluginOutputModeIds), ", "))).
		AddBoolFlag(pconstants.ArgHelp, false, "Help for table list", cmdconfig.FlagOptions.WithShortHand("h"))

	return cmd
}

func runTableListCmd(cmd *cobra.Command, args []string) {
	//setup a cancel context and start cancel handler
	ctx, cancel := context.WithCancel(cmd.Context())
	contexthelpers.StartCancelHandler(cancel)
	utils.LogTime("runSourceListCmd start")
	defer func() {
		utils.LogTime("runSourceListCmd end")
		if r := recover(); r != nil {
			error_helpers.ShowError(ctx, helpers.ToError(r))
			exitCode = pconstants.ExitCodeUnknownErrorPanic
		}
	}()

	db, err := database.NewDuckDb(database.WithDuckLakeEnabled(true))
	error_helpers.FailOnError(err)
	defer db.Close()

	// Get Resources
	resources, err := display.ListTableResources(ctx, db)
	error_helpers.FailOnError(err)
	printableResource := display.NewPrintableResource(resources...)

	// Get Printer
	printer, err := printers.GetPrinter[*display.TableResource](cmd)
	error_helpers.FailOnError(err)

	// Print
	err = printer.PrintResource(ctx, printableResource, cmd.OutOrStdout())
	if err != nil {
		error_helpers.ShowError(ctx, err)
		exitCode = pconstants.ExitCodeUnknownErrorPanic
	}
}

// Show Table
func tableShowCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "show <table>",
		Args:  cobra.ExactArgs(1),
		Run:   runTableShowCmd,
		Short: "Show details for a specific table.",
		Long:  `Show details for a specific table.`,
	}

	cmdconfig.
		OnCmd(cmd).
		AddVarFlag(enumflag.New(&pluginOutputMode, pconstants.ArgOutput, constants.PluginOutputModeIds, enumflag.EnumCaseInsensitive),
			pconstants.ArgOutput,
			fmt.Sprintf("Output format; one of: %s", strings.Join(constants.FlagValues(constants.PluginOutputModeIds), ", "))).
		AddBoolFlag(pconstants.ArgHelp, false, "Help for table show", cmdconfig.FlagOptions.WithShortHand("h"))

	return cmd
}

func runTableShowCmd(cmd *cobra.Command, args []string) {
	//setup a cancel context and start cancel handler
	ctx, cancel := context.WithCancel(cmd.Context())
	contexthelpers.StartCancelHandler(cancel)
	utils.LogTime("runTableShowCmd start")
	defer func() {
		utils.LogTime("runTableShowCmd end")
		if r := recover(); r != nil {
			error_helpers.ShowError(ctx, helpers.ToError(r))
			exitCode = pconstants.ExitCodeUnknownErrorPanic
		}
	}()

	db, err := database.NewDuckDb(database.WithDuckLakeEnabled(true))
	error_helpers.FailOnError(err)
	defer db.Close()

	// Get Resources
	resource, err := display.GetTableResource(ctx, args[0], db)
	error_helpers.FailOnError(err)
	printableResource := display.NewPrintableResource(resource)

	// Get Printer
	printer, err := printers.GetPrinter[*display.TableResource](cmd)
	error_helpers.FailOnError(err)

	// Print
	err = printer.PrintResource(ctx, printableResource, cmd.OutOrStdout())
	if err != nil {
		error_helpers.ShowError(ctx, err)
		exitCode = pconstants.ExitCodeUnknownErrorPanic
	}
}
