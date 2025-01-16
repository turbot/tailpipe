package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/danwakefield/fnmatch"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/cmdconfig"
	"github.com/turbot/pipe-fittings/constants"
	pconstants "github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/pipe-fittings/parse"
	"github.com/turbot/tailpipe/internal/collector"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/plugin_manager"
	"golang.org/x/exp/maps"
)

// NOTE: the hard coded config that was previously defined here has been moved to hcl in the file tailpipe/internal/parse/test_data/configs/resources.tpc
// to reference this use: collect --config-path <path to tailpipe>/internal/parse/test_data/configs --partition aws_cloudtrail_log.cloudtrail_logs

func collectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "collect [flags]",
		Args:             cobra.ArbitraryArgs,
		TraverseChildren: true,
		Run:              runCollectCmd,
		Short:            "Collect logs from configured sources",
		Long:             `Collect logs from configured sources.`,
	}
	// arg `from` accepts:
	// - ISO 8601 date (2024-01-01)
	// - ISO 8601 datetime (2006-01-02T15:04:05)
	// - ISO 8601 datetime with ms (2006-01-02T15:04:05.000)
	// - RFC 3339 datetime with timezone (2006-01-02T15:04:05Z07:00)
	// - relative time formats (T-2Y, T-10m, T-10W, T-180d, T-9H, T-10M)

	cmdconfig.OnCmd(cmd).
		AddBoolFlag(pconstants.ArgCompact, true, "Compact the parquet files after collection").
		AddStringFlag(pconstants.ArgFrom, "", "Specify the collection start time").
		AddBoolFlag(pconstants.ArgTiming, false, "Show timing information")

	return cmd
}

func runCollectCmd(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	var err error
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}

		if err != nil {
			error_helpers.ShowError(ctx, err)
			setExitCodeForCollectError(err)
		}
	}()

	err = collectAndCompact(ctx, args)
	if errors.Is(err, context.Canceled) {
		// clear error so we don't show it with normal error reporting
		err = nil
		fmt.Println("Collection cancelled.") //nolint:forbidigo // ui output
	}
}

func collectAndCompact(ctx context.Context, args []string) error {
	// collect the data
	statusStrings, timingStrings, err := doCollect(ctx, args)
	if err != nil {
		return err
	}

	// compact the data
	var compactStatusString string
	if viper.GetBool(pconstants.ArgCompact) {
		compactStatus, err := doCompaction(ctx)
		// if the context was cancelled, we don't want to return an error
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("compaction error: %w", err)
		}
		compactStatusString = compactStatus.BriefString()
		if ctx.Err() != nil {
			// instead show the status as cancelled
			compactStatusString = "Compaction cancelled: " + compactStatusString
		}
	}

	// now show the result
	for i, statusString := range statusStrings {
		fmt.Println(statusString) //nolint:forbidigo // ui output
		// show timing if requested
		if len(timingStrings) > i && shouldShowCollectTiming() {
			fmt.Println(timingStrings[i]) //nolint:forbidigo // ui output
		}
	}
	if compactStatusString != "" {
		fmt.Println(compactStatusString) //nolint:forbidigo // ui output
	}

	return nil
}

func doCollect(ctx context.Context, args []string) ([]string, []string, error) {
	var fromTime time.Time
	if viper.GetString(pconstants.ArgFrom) != "" {
		var err error
		fromTime, err = parse.ParseTime(viper.GetString(pconstants.ArgFrom), time.Now())
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse 'from' argument: %w", err)
		}
	}

	partitions, err := getPartitions(args)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get partition config: %w", err)
	}

	// now we have the partitions, we can start collecting

	// start the plugin manager
	pluginManager := plugin_manager.New()
	defer pluginManager.Close()

	// collect each partition serially
	statusStrings := make([]string, 0, len(partitions))
	timingStrings := make([]string, 0, len(partitions))
	var errList []error
	for _, partition := range partitions {
		statusString, timingString, err := collectPartition(ctx, partition, fromTime, pluginManager)
		if err != nil {
			errList = append(errList, err)
		} else {
			statusStrings = append(statusStrings, statusString)
			timingStrings = append(timingStrings, timingString)
		}
	}

	if len(errList) > 0 {
		err = errors.Join(errList...)
		return nil, nil, fmt.Errorf("collection error: %w", err)
	}

	return statusStrings, timingStrings, nil
}

func collectPartition(ctx context.Context, partition *config.Partition, fromTime time.Time, pluginManager *plugin_manager.PluginManager) (string, string, error) {
	c, err := collector.New(pluginManager)
	if err != nil {
		return "", "", fmt.Errorf("failed to create collector: %w", err)
	}
	defer c.Close()

	// if there is a from time, add a filter to the partition
	if !fromTime.IsZero() {
		partition.AddFilter(fmt.Sprintf("tp_timestamp >= '%s'", fromTime.Format("2006-01-02T15:04:05")))
	}

	if err := c.Collect(ctx, partition, fromTime); err != nil {
		return "", "", err
	}
	// now wait for all collection to complete and close the collector
	c.WaitForCompletion(ctx)
	return c.StatusString(), c.TimingString(), nil
}

func getPartitions(args []string) ([]*config.Partition, error) {
	// we have loaded tailpipe config by this time
	tailpipeConfig := config.GlobalConfig

	// if no partitions specified, return all
	if len(args) == 0 {
		return maps.Values(tailpipeConfig.Partitions), nil
	}

	var errorList []error
	var partitions []*config.Partition

	for _, arg := range args {
		partitionNames, err := getPartitionsForArg(maps.Keys(tailpipeConfig.Partitions), arg)
		if err != nil {
			errorList = append(errorList, err)
		} else if len(partitionNames) == 0 {
			errorList = append(errorList, fmt.Errorf("partition not found: %s", arg))
		} else {
			for _, partitionName := range partitionNames {
				partitions = append(partitions, tailpipeConfig.Partitions[partitionName])
			}
		}
	}

	if len(errorList) > 0 {
		// TODO #errors better formating/error message https://github.com/turbot/tailpipe/issues/106
		return nil, errors.Join(errorList...)
	}

	return partitions, nil
}

func getPartitionsForArg(partitions []string, arg string) ([]string, error) {
	var tablePattern, partitionPattern string
	parts := strings.Split(arg, ".")
	switch len(parts) {
	case 1:
		var err error
		tablePattern, partitionPattern, err = getPartitionMatchPatterns(partitions, arg, parts)
		if err != nil {
			return nil, err
		}
	case 2:
		// use the args as provided
		tablePattern = parts[0]
		partitionPattern = parts[1]
	default:
		return nil, fmt.Errorf("invalid partition name: %s", arg)
	}

	// now match the partition
	var res []string
	for _, partition := range partitions {
		pattern := tablePattern + "." + partitionPattern
		if fnmatch.Match(pattern, partition, fnmatch.FNM_CASEFOLD) {
			res = append(res, partition)
		}
	}
	return res, nil
}

func getPartitionMatchPatterns(partitions []string, arg string, parts []string) (string, string, error) {
	var tablePattern, partitionPattern string
	// '*' is not valid for a single part arg
	if parts[0] == "*" {
		return "", "", fmt.Errorf("invalid partition name: %s", arg)
	}
	// check whether there is table with this name
	// partitions is a list of Unqualified names, i.e. <table>.<partition>
	for _, partition := range partitions {
		table := strings.Split(partition, ".")[0]

		// if the arg matches a table name, set table pattern to the arg and partition pattern to *
		if fnmatch.Match(arg, table, fnmatch.FNM_CASEFOLD) {
			tablePattern = arg
			partitionPattern = "*"
			return tablePattern, partitionPattern, nil
		}
	}
	// so there IS NOT a table with this name - set table pattern to * and user provided partition name
	tablePattern = "*"
	partitionPattern = parts[0]
	return tablePattern, partitionPattern, nil
}

func setExitCodeForCollectError(err error) {
	// if exit code already set, leave as is
	if exitCode != 0 || err == nil {
		return
	}

	// TODO #errors - assign exit codes https://github.com/turbot/tailpipe/issues/106
	exitCode = 1
}

func shouldShowCollectTiming() bool {
	return viper.GetBool(constants.ArgTiming)
}
