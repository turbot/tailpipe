package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/thediveo/enumflag/v2"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/contexthelpers"
	"github.com/turbot/pipe-fittings/v2/printers"
	"github.com/turbot/pipe-fittings/v2/utils"
	localcmdconfig "github.com/turbot/tailpipe/internal/cmdconfig"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/display"
	error_helpers "github.com/turbot/tailpipe/internal/error_helpers"
)

// variable used to assign the output mode flag
var formatOutputMode = constants.PluginOutputModePretty

func formatCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "format [command]",
		Args:  cobra.NoArgs,
		Short: "List and show Tailpipe formats",
		Long: `Tailpipe format commands.
 
 Formats define how data is structured and processed in Tailpipe.
 
 Examples:
 
   # List all formats
   tailpipe format list
 
   # Show details for a specific format
   tailpipe format show grok.custom_log`,
	}

	cmd.AddCommand(formatListCmd())
	cmd.AddCommand(formatShowCmd())

	cmd.Flags().BoolP(pconstants.ArgHelp, "h", false, "Help for format")

	return cmd
}

// List Formats
func formatListCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "list",
		Args:  cobra.NoArgs,
		Run:   runFormatListCmd,
		Short: "List all formats.",
		Long:  `List all formats.`,
	}

	cmdconfig.
		OnCmd(cmd).
		AddVarFlag(enumflag.New(&formatOutputMode, pconstants.ArgOutput, constants.PluginOutputModeIds, enumflag.EnumCaseInsensitive),
			pconstants.ArgOutput,
			fmt.Sprintf("Output format; one of: %s", strings.Join(constants.FlagValues(constants.PluginOutputModeIds), ", "))).
		AddBoolFlag(pconstants.ArgHelp, false, "Help for format list", cmdconfig.FlagOptions.WithShortHand("h"))
	return cmd
}

func runFormatListCmd(cmd *cobra.Command, args []string) {
	//setup a cancel context and start cancel handler
	ctx, cancel := context.WithCancel(cmd.Context())
	contexthelpers.StartCancelHandler(cancel)
	utils.LogTime("runFormatListCmd start")
	var err error
	defer func() {
		utils.LogTime("runFormatListCmd end")
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
		if err != nil {
			if error_helpers.IsCancelledError(err) {
				//nolint:forbidigo // ui output
				fmt.Println("Format cancelled.")
			} else {
				error_helpers.ShowError(ctx, err)
			}
			setExitCodeForFormatError(err)
		}
	}()

	// if diagnostic mode is set, print out config and return
	if _, ok := os.LookupEnv(constants.EnvConfigDump); ok {
		localcmdconfig.DisplayConfig()
		return
	}

	// Get Resources
	resources, err := display.ListFormatResources(ctx)
	error_helpers.FailOnError(err)
	printableResource := display.NewPrintableResource(resources...)

	// Get Printer
	printer, err := printers.GetPrinter[*display.FormatResource](cmd)
	error_helpers.FailOnError(err)

	// Print
	err = printer.PrintResource(ctx, printableResource, cmd.OutOrStdout())
	if err != nil {
		exitCode = pconstants.ExitCodeOutputRenderingFailed
		return
	}
}

// Show Format
func formatShowCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "show [format]",
		Args:  cobra.ExactArgs(1),
		Run:   runFormatShowCmd,
		Short: "Show details for a specific format",
		Long:  `Show details for a specific format.`,
	}

	cmdconfig.
		OnCmd(cmd).
		AddVarFlag(enumflag.New(&formatOutputMode, pconstants.ArgOutput, constants.PluginOutputModeIds, enumflag.EnumCaseInsensitive),
			pconstants.ArgOutput,
			fmt.Sprintf("Output format; one of: %s", strings.Join(constants.FlagValues(constants.PluginOutputModeIds), ", "))).
		AddBoolFlag(pconstants.ArgHelp, false, "Help for format show", cmdconfig.FlagOptions.WithShortHand("h"))
	return cmd
}

func runFormatShowCmd(cmd *cobra.Command, args []string) {
	//setup a cancel context and start cancel handler
	ctx, cancel := context.WithCancel(cmd.Context())
	contexthelpers.StartCancelHandler(cancel)
	utils.LogTime("runFormatShowCmd start")
	var err error
	defer func() {
		utils.LogTime("runFormatShowCmd end")
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
		if err != nil {
			if error_helpers.IsCancelledError(err) {
				//nolint:forbidigo // ui output
				fmt.Println("Format cancelled.")
			} else {
				error_helpers.ShowError(ctx, err)
			}
			setExitCodeForFormatError(err)
		}
	}()

	// Get Resources
	resourceName := args[0]
	resource, err := display.GetFormatResource(ctx, resourceName)
	error_helpers.FailOnError(err)
	printableResource := display.NewPrintableResource(resource)

	// Get Printer
	printer, err := printers.GetPrinter[*display.FormatResource](cmd)
	error_helpers.FailOnError(err)

	// Print
	err = printer.PrintResource(ctx, printableResource, cmd.OutOrStdout())
	if err != nil {
		exitCode = pconstants.ExitCodeOutputRenderingFailed
		return
	}
}

func setExitCodeForFormatError(err error) {
	// set exit code only if an error occurred and no exit code is already set
	if exitCode != 0 || err == nil {
		return
	}
	// set exit code for cancellation
	if error_helpers.IsCancelledError(err) {
		exitCode = pconstants.ExitCodeOperationCancelled
		return
	}
	// no dedicated format exit code exists yet; use generic nonzero failure
	exitCode = 1
}
