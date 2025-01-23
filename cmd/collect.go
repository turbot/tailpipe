package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/danwakefield/fnmatch"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/pipe-fittings/parse"
	"github.com/turbot/tailpipe/internal/collector"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/parquet"
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
	err := doCollect(ctx, args)
	if err != nil {
		return err
	}

	return nil
}

func doCollect(ctx context.Context, args []string) error {
	// arg `from` accepts ISO 8601 date(2024-01-01), ISO 8601 datetime(2006-01-02T15:04:05), ISO 8601 datetime with ms(2006-01-02T15:04:05.000),
	// RFC 3339 datetime with timezone(2006-01-02T15:04:05Z07:00) and relative time formats(T-2Y, T-10m, T-10W, T-180d, T-9H, T-10M)
	var fromTime time.Time
	if viper.IsSet(pconstants.ArgFrom) {
		var err error
		fromTime, err = parseFromTime(viper.GetString(pconstants.ArgFrom), time.Hour*24)
		if err != nil {
			return err
		}
	}
	partitions, err := getPartitions(args)
	if err != nil {
		return fmt.Errorf("failed to get partition config: %w", err)
	}

	// now we have the partitions, we can start collecting

	// start the plugin manager
	pluginManager := plugin_manager.New()
	defer pluginManager.Close()

	// collect each partition serially
	var errList []error
	for _, partition := range partitions {
		// if a from time is set, clear the partition data from that time forward
		if !fromTime.IsZero() {
			_, err := parquet.DeleteParquetFiles(partition, fromTime)
			if err != nil {
				slog.Warn("Failed to delete parquet files after the from time", "partition", partition.Name, "from", fromTime, "error", err)
				errList = append(errList, err)
				continue
			}
			error_helpers.FailOnError(err)
		}
		// do the collection
		err = collectPartition(ctx, partition, fromTime, pluginManager)
		if err != nil {
			errList = append(errList, err)
		}
	}

	if len(errList) > 0 {
		err = errors.Join(errList...)
		return fmt.Errorf("collection error: %w", err)
	}

	return nil
}

func collectPartition(ctx context.Context, partition *config.Partition, fromTime time.Time, pluginManager *plugin_manager.PluginManager) error {
	c, err := collector.New(pluginManager)
	if err != nil {
		return fmt.Errorf("failed to create collector: %w", err)
	}
	defer c.Close()

	if err = c.Collect(ctx, partition, fromTime); err != nil {
		return err
	}

	// now wait for all collection to complete and close the collector
	c.WaitForCompletion(ctx)

	err = c.Compact(ctx)
	if err != nil {
		return err
	}

	return nil
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

// parse the from time, validating the granularity
// for example, if the from arg is T-4H and the granularity is 1 day, that is an error
func parseFromTime(fromArg string, granularity time.Duration) (time.Time, error) {
	now := time.Now()

	fromTime, err := parse.ParseTime(fromArg, now)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse 'from' argument: %w", err)
	}
	// ensure the from time passed is more than the granularity away from now
	// and truncate to the granularity
	if time.Since(fromTime) < granularity {
		return time.Time{}, fmt.Errorf("'from' time must be at least %s in the past", formatDuration(granularity))
	}
	return fromTime.Truncate(granularity), nil
}

// HumanizeDuration converts a time.Duration into a human-readable string
func formatDuration(d time.Duration) string {
	if d.Hours() >= 24 {
		days := int(d.Hours() / 24)
		if days == 1 {
			return "1 day"
		}
		return fmt.Sprintf("%d days", days)
	} else if d.Hours() >= 1 {
		hours := int(d.Hours())
		if hours == 1 {
			return "1 hour"
		}
		return fmt.Sprintf("%d hours", hours)
	} else if d.Minutes() >= 1 {
		minutes := int(d.Minutes())
		if minutes == 1 {
			return "1 minute"
		}
		return fmt.Sprintf("%d minutes", minutes)
	} else {
		seconds := int(d.Seconds())
		if seconds == 1 {
			return "1 second"
		}
		return fmt.Sprintf("%d seconds", seconds)
	}
}
