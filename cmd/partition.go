package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thediveo/enumflag/v2"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/contexthelpers"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/pipe-fittings/v2/printers"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/display"
	"github.com/turbot/tailpipe/internal/filepaths"
	"github.com/turbot/tailpipe/internal/parquet"
	"github.com/turbot/tailpipe/internal/plugin_manager"
)

func partitionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "partition [command]",
		Args:  cobra.NoArgs,
		Short: "List, show, and delete Tailpipe partitions",
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
	cmd.AddCommand(partitionDeleteCmd())
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
		Short: "Show details for a specific partition",
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
	resource, err := display.GetPartitionResource(partitionName)
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

func partitionDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete ",
		Args:  cobra.ExactArgs(1),
		Run:   runPartitionDeleteCmd,
		Short: "Delete a partition for the specified period",
		Long:  `Delete a partition for the specified period`,
	}

	// args `from` and `to` accept:
	// - ISO 8601 date (2024-01-01)
	// - ISO 8601 datetime (2006-01-02T15:04:05)
	// - ISO 8601 datetime with ms (2006-01-02T15:04:05.000)
	// - RFC 3339 datetime with timezone (2006-01-02T15:04:05Z07:00)
	// - relative time formats (T-2Y, T-10m, T-10W, T-180d, T-9H, T-10M)

	cmdconfig.OnCmd(cmd).
		AddStringFlag(pconstants.ArgFrom, "", "Specify the start time").
		AddBoolFlag(pconstants.ArgForce, false, "Force delete without confirmation")

	return cmd
}

func runPartitionDeleteCmd(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	defer func() {
		if r := recover(); r != nil {
			exitCode = pconstants.ExitCodeUnknownErrorPanic
			error_helpers.FailOnError(helpers.ToError(r))
		}
	}()

	// arg `fromTime` accepts ISO 8601 date(2024-01-01), ISO 8601 datetime(2006-01-02T15:04:05), ISO 8601 datetime with ms(2006-01-02T15:04:05.000),
	// RFC 3339 datetime with timezone(2006-01-02T15:04:05Z07:00) and relative time formats(T-2Y, T-10m, T-10W, T-180d, T-9H, T-10M)
	var fromTime time.Time
	var fromStr string
	if viper.IsSet(pconstants.ArgFrom) {
		var err error
		fromTime, err = parseFromTime(viper.GetString(pconstants.ArgFrom), time.Hour*24)
		error_helpers.FailOnError(err)
		fromStr = fmt.Sprintf(" from %s", fromTime.Format(time.DateOnly))
	}

	partitionName := args[0]
	partition, ok := config.GlobalConfig.Partitions[partitionName]
	if !ok {
		error_helpers.FailOnError(fmt.Errorf("partition %s not found", partitionName))
	}

	if !viper.GetBool(pconstants.ArgForce) {
		// confirm deletion
		msg := fmt.Sprintf("Are you sure you want to delete partition %s%s?", partitionName, fromStr)
		if !utils.UserConfirmationWithDefault(msg, true) {
			fmt.Println("Deletion cancelled") //nolint:forbidigo//expected output
			return
		}
	}

	filesDeleted, err := parquet.DeleteParquetFiles(partition, fromTime)
	error_helpers.FailOnError(err)

	// build the collection state path
	collectionStatePath := partition.CollectionStatePath(config.GlobalWorkspaceProfile.GetCollectionDir())

	// if the fromTime time is not specified, just delete the colleciton state file
	if fromTime.IsZero() {
		err := os.Remove(collectionStatePath)
		if err != nil && !os.IsNotExist(err) {
			error_helpers.FailOnError(fmt.Errorf("failed to delete collection state file: %s", err.Error()))
		}
	} else {
		// update collection state
		// start the plugin manager
		pluginManager := plugin_manager.New()
		defer pluginManager.Close()
		err = pluginManager.UpdateCollectionState(ctx, partition, fromTime, collectionStatePath)
		error_helpers.FailOnError(err)
	}

	// now prune the collection folders
	err = filepaths.PruneTree(config.GlobalWorkspaceProfile.GetCollectionDir())
	if err != nil {
		slog.Warn("DeleteParquetFiles failed to prune empty collection folders", "error", err)
	}

	msg := buildStatusMessage(filesDeleted, partitionName, fromStr)
	fmt.Println(msg) //nolint:forbidigo//expected output
}

func buildStatusMessage(filesDeleted int, partition string, fromStr string) interface{} {
	var deletedStr = " (no parquet files deleted)"
	if filesDeleted > 0 {
		deletedStr = fmt.Sprintf(" (deleted %d parquet %s)", filesDeleted, utils.Pluralize("file", filesDeleted))
	}

	return fmt.Sprintf("\nDeleted partition '%s'%s%s.\n", partition, fromStr, deletedStr)
}
