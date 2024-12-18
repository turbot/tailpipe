package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/thediveo/enumflag/v2"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/contexthelpers"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/pipe-fittings/printers"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/display"
)

func partitionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "partition [command]",
		Args:  cobra.NoArgs,
		Short: "Tailpipe partition commands.",
		Long: `Tailpipe partition commands.

Partitions are instances of Tailpipe tables with a defined source and configuration.

Examples:

	# List all partitions
	tailpipe partition list

	# Show details for a specific partition
	tailpipe partition show aws_cloudtrail_log.account_a`,
	}

	cmd.AddCommand(partitionListCmd())
	cmd.AddCommand(partitionShowCmd())

	cmd.Flags().BoolP(pconstants.ArgHelp, "h", false, "Help for partition")

	return cmd
}

// List Partitions
func partitionListCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "list",
		Args:  cobra.NoArgs,
		Run:   runPartitionListCmd,
		Short: "List all partitions.",
		Long:  `List all partitions.`,
	}

	cmdconfig.
		OnCmd(cmd).
		AddVarFlag(enumflag.New(&pluginOutputMode, pconstants.ArgOutput, constants.PluginOutputModeIds, enumflag.EnumCaseInsensitive),
			pconstants.ArgOutput,
			fmt.Sprintf("Output format; one of: %s", strings.Join(constants.FlagValues(constants.PluginOutputModeIds), ", "))).
		AddBoolFlag(pconstants.ArgHelp, false, "Help for partition list", cmdconfig.FlagOptions.WithShortHand("h"))

	return cmd
}

func runPartitionListCmd(cmd *cobra.Command, args []string) {
	//setup a cancel context and start cancel handler
	ctx, cancel := context.WithCancel(cmd.Context())
	contexthelpers.StartCancelHandler(cancel)
	utils.LogTime("runPartitionListCmd start")
	defer func() {
		utils.LogTime("runPartitionListCmd end")
		if r := recover(); r != nil {
			error_helpers.ShowError(ctx, helpers.ToError(r))
			exitCode = pconstants.ExitCodeUnknownErrorPanic
		}
	}()

	// Get Resources
	resources, err := display.ListPartitionResources(ctx)
	error_helpers.FailOnError(err)
	printableResource := display.NewPrintableResource(resources...)

	// Get Printer
	printer, err := printers.GetPrinter[*display.PartitionResource](cmd)
	error_helpers.FailOnError(err)

	// Print
	err = printer.PrintResource(ctx, printableResource, cmd.OutOrStdout())
	if err != nil {
		error_helpers.ShowError(ctx, err)
		exitCode = pconstants.ExitCodeUnknownErrorPanic
	}
}

// Show Partition
func partitionShowCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "show",
		Args:  cobra.ExactArgs(1),
		Run:   runPartitionShowCmd,
		Short: "Show details for a specific partition.",
		Long:  `Show details for a specific partition.`,
	}

	cmdconfig.
		OnCmd(cmd).
		AddVarFlag(enumflag.New(&pluginOutputMode, pconstants.ArgOutput, constants.PluginOutputModeIds, enumflag.EnumCaseInsensitive),
			pconstants.ArgOutput,
			fmt.Sprintf("Output format; one of: %s", strings.Join(constants.FlagValues(constants.PluginOutputModeIds), ", "))).
		AddBoolFlag(pconstants.ArgHelp, false, "Help for partition show", cmdconfig.FlagOptions.WithShortHand("h"))

	return cmd
}

func runPartitionShowCmd(cmd *cobra.Command, args []string) {
	//setup a cancel context and start cancel handler
	ctx, cancel := context.WithCancel(cmd.Context())
	contexthelpers.StartCancelHandler(cancel)
	utils.LogTime("runPartitionShowCmd start")
	defer func() {
		utils.LogTime("runPartitionShowCmd end")
		if r := recover(); r != nil {
			error_helpers.ShowError(ctx, helpers.ToError(r))
			exitCode = pconstants.ExitCodeUnknownErrorPanic
		}
	}()

	// Get Resources
	partitionName := args[0]
	resource, err := display.GetPartitionResource(ctx, partitionName)
	error_helpers.FailOnError(err)
	printableResource := display.NewPrintableResource(resource)

	// Get Printer
	printer, err := printers.GetPrinter[*display.PartitionResource](cmd)
	error_helpers.FailOnError(err)

	// Print
	err = printer.PrintResource(ctx, printableResource, cmd.OutOrStdout())
	if err != nil {
		error_helpers.ShowError(ctx, err)
		exitCode = pconstants.ExitCodeUnknownErrorPanic
	}
}
