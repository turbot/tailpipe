package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/thediveo/enumflag/v2"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/printers"
	"github.com/turbot/pipe-fittings/v2/utils"
	localcmdconfig "github.com/turbot/tailpipe/internal/cmdconfig"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/display"
	"github.com/turbot/tailpipe/internal/error_helpers"
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
	// use the signal-aware/cancelable context created upstream in preRunHook
	ctx := cmd.Context()
	utils.LogTime("runSourceListCmd start")
	var err error
	defer func() {
		utils.LogTime("runSourceListCmd end")
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
		if err != nil {
			if error_helpers.IsCancelledError(err) {
				//nolint:forbidigo // ui output
				fmt.Println("tailpipe table list command cancelled.")
			} else {
				error_helpers.ShowError(ctx, err)
			}
			setExitCodeForTableError(err)
		}
	}()

	// if diagnostic mode is set, print out config and return
	if _, ok := os.LookupEnv(constants.EnvConfigDump); ok {
		localcmdconfig.DisplayConfig()
		return
	}

	// open a readonly db connection
	db, err := database.NewDuckDb(database.WithDuckLakeReadonly())
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
		exitCode = pconstants.ExitCodeOutputRenderingFailed
		return
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
	// use the signal-aware/cancelable context created upstream in preRunHook
	ctx := cmd.Context()
	utils.LogTime("runTableShowCmd start")
	var err error
	defer func() {
		utils.LogTime("runTableShowCmd end")
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
		if err != nil {
			if error_helpers.IsCancelledError(err) {
				//nolint:forbidigo // ui output
				fmt.Println("tailpipe table show command cancelled.")
			} else {
				error_helpers.ShowError(ctx, err)
			}
			setExitCodeForTableError(err)
		}
	}()

	// if diagnostic mode is set, print out config and return
	if _, ok := os.LookupEnv(constants.EnvConfigDump); ok {
		localcmdconfig.DisplayConfig()
		return
	}

	// open a readonly db connection
	db, err := database.NewDuckDb(database.WithDuckLakeReadonly())
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
		exitCode = pconstants.ExitCodeOutputRenderingFailed
		return
	}
}

func setExitCodeForTableError(err error) {
	if exitCode != 0 || err == nil {
		return
	}
	if error_helpers.IsCancelledError(err) {
		exitCode = pconstants.ExitCodeOperationCancelled
		return
	}
	exitCode = 1
}
