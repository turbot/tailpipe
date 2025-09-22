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
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/pipe-fittings/v2/printers"
	"github.com/turbot/pipe-fittings/v2/utils"
	localcmdconfig "github.com/turbot/tailpipe/internal/cmdconfig"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/display"
)

func sourceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "source [command]",
		Args:  cobra.NoArgs,
		Short: "List and show Tailpipe sources",
		Long: `Tailpipe source commands.

Sources extend Tailpipe to obtain data from many different services and providers.

Examples:

  # List all sources
  tailpipe source list

  # Show details for a specific source
  tailpipe source show aws_s3_bucket`,
	}

	cmd.AddCommand(sourceListCmd())
	cmd.AddCommand(sourceShowCmd())

	cmd.Flags().BoolP(pconstants.ArgHelp, "h", false, "Help for source")

	return cmd
}

// List Sources
func sourceListCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "list",
		Args:  cobra.NoArgs,
		Run:   runSourceListCmd,
		Short: "List all sources.",
		Long:  `List all sources.`,
	}

	cmdconfig.
		OnCmd(cmd).
		AddVarFlag(enumflag.New(&pluginOutputMode, pconstants.ArgOutput, constants.PluginOutputModeIds, enumflag.EnumCaseInsensitive),
			pconstants.ArgOutput,
			fmt.Sprintf("Output format; one of: %s", strings.Join(constants.FlagValues(constants.PluginOutputModeIds), ", "))).
		AddBoolFlag(pconstants.ArgHelp, false, "Help for source list", cmdconfig.FlagOptions.WithShortHand("h"))
	return cmd
}

func runSourceListCmd(cmd *cobra.Command, args []string) {
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
				fmt.Println("tailpipe source list command cancelled.")
			} else {
				error_helpers.ShowError(ctx, err)
			}
			setExitCodeForSourceError(err)
		}
	}()

	// if diagnostic mode is set, print out config and return
	if _, ok := os.LookupEnv(constants.EnvConfigDump); ok {
		localcmdconfig.DisplayConfig()
		return
	}

	// Get Resources
	resources, err := display.ListSourceResources(ctx)
	error_helpers.FailOnError(err)
	printableResource := display.NewPrintableResource(resources...)

	// Get Printer
	printer, err := printers.GetPrinter[*display.SourceResource](cmd)
	error_helpers.FailOnError(err)

	// Print
	err = printer.PrintResource(ctx, printableResource, cmd.OutOrStdout())
	if err != nil {
		exitCode = pconstants.ExitCodeOutputRenderingFailed
		return
	}
}

// Show Source
func sourceShowCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "show [source]",
		Args:  cobra.ExactArgs(1),
		Run:   runSourceShowCmd,
		Short: "Show details for a specific source",
		Long:  `Show details for a specific source.`,
	}

	cmdconfig.
		OnCmd(cmd).
		AddVarFlag(enumflag.New(&pluginOutputMode, pconstants.ArgOutput, constants.PluginOutputModeIds, enumflag.EnumCaseInsensitive),
			pconstants.ArgOutput,
			fmt.Sprintf("Output format; one of: %s", strings.Join(constants.FlagValues(constants.PluginOutputModeIds), ", "))).
		AddBoolFlag(pconstants.ArgHelp, false, "Help for source show", cmdconfig.FlagOptions.WithShortHand("h"))
	return cmd
}

func runSourceShowCmd(cmd *cobra.Command, args []string) {
	// use the signal-aware/cancelable context created upstream in preRunHook
	// TODO: https://github.com/turbot/tailpipe/issues/563 none of the functions called in this command will return a
	// cancellation error. Cancellation won't work right now
	ctx := cmd.Context()
	utils.LogTime("runSourceShowCmd start")
	var err error
	defer func() {
		utils.LogTime("runSourceShowCmd end")
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
		if err != nil {
			if error_helpers.IsCancelledError(err) {
				//nolint:forbidigo // ui output
				fmt.Println("tailpipe source show command cancelled.")
			} else {
				error_helpers.ShowError(ctx, err)
			}
			setExitCodeForSourceError(err)
		}
	}()

	// if diagnostic mode is set, print out config and return
	if _, ok := os.LookupEnv(constants.EnvConfigDump); ok {
		localcmdconfig.DisplayConfig()
		return
	}

	// Get Resources
	resourceName := args[0]
	resource, err := display.GetSourceResource(ctx, resourceName)
	error_helpers.FailOnError(err)
	printableResource := display.NewPrintableResource(resource)

	// Get Printer
	printer, err := printers.GetPrinter[*display.SourceResource](cmd)
	error_helpers.FailOnError(err)

	// Print
	err = printer.PrintResource(ctx, printableResource, cmd.OutOrStdout())
	if err != nil {
		exitCode = pconstants.ExitCodeOutputRenderingFailed
		return
	}
}

func setExitCodeForSourceError(err error) {
	if exitCode != 0 || err == nil {
		return
	}
	if error_helpers.IsCancelledError(err) {
		exitCode = pconstants.ExitCodeOperationCancelled
		return
	}
	exitCode = 1
}
