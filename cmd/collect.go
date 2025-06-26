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
	"github.com/turbot/pipe-fittings/v2/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/contexthelpers"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/pipe-fittings/v2/parse"
	"github.com/turbot/tailpipe/internal/collector"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/parquet"
	"github.com/turbot/tailpipe/internal/plugin"
	"golang.org/x/exp/maps"
)

// NOTE: the hard coded config that was previously defined here has been moved to hcl in the file tailpipe/internal/parse/test_data/configs/resources.tpc
// to reference this use: collect --config-path <path to tailpipe>/internal/parse/test_data/configs --partition aws_cloudtrail_log.cloudtrail_logs

func collectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "collect [table|table.partition] [flags]",
		Args:             cobra.ArbitraryArgs,
		TraverseChildren: true,
		Run:              runCollectCmd,
		Short:            "Run a collection",
		Long: `The tailpipe collect command runs a plugin that reads from a source and writes to the hive. 
		
Every time you run tailpipe collect, Tailpipe refreshes its views over all collected parquet files.`,
	}
	// arg `from` accepts:
	// - ISO 8601 date (2024-01-01)
	// - ISO 8601 datetime (2006-01-02T15:04:05)
	// - ISO 8601 datetime with ms (2006-01-02T15:04:05.000)
	// - RFC 3339 datetime with timezone (2006-01-02T15:04:05Z07:00)
	// - relative time formats (T-2Y, T-10m, T-10W, T-180d, T-9H, T-10M)

	cmdconfig.OnCmd(cmd).
		AddBoolFlag(pconstants.ArgCompact, true, "Compact the parquet files after collection").
		AddStringFlag(pconstants.ArgFrom, "", "Collect days newer than a relative or absolute date (collection defaulting to 7 days if not specified)").
		AddStringFlag(pconstants.ArgTo, "", "Collect days older than a relative or absolute date (defaulting to now if not specified)").
		AddBoolFlag(pconstants.ArgProgress, true, "Show active progress of collection, set to false to disable").
		AddBoolFlag(pconstants.ArgOverwrite, false, "Recollect data from the source even if it has already been collected")

	return cmd
}

func runCollectCmd(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(cmd.Context())
	contexthelpers.StartCancelHandler(cancel)

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

	err = doCollect(ctx, cancel, args)
	if errors.Is(err, context.Canceled) {
		// clear error so we don't show it with normal error reporting
		err = nil
		fmt.Println("Collection cancelled.") //nolint:forbidigo // ui output
	}
}

func doCollect(ctx context.Context, cancel context.CancelFunc, args []string) error {
	// arg `from` accepts ISO 8601 date(2024-01-01), ISO 8601 datetime(2006-01-02T15:04:05), ISO 8601 datetime with ms(2006-01-02T15:04:05.000),
	// RFC 3339 datetime with timezone(2006-01-02T15:04:05Z07:00) and relative time formats(T-2Y, T-10m, T-10W, T-180d, T-9H, T-10M)
	var fromTime time.Time
	// toTime defaults to now, but can be set to a specific time
	toTime := time.Now()
	var err error
	if viper.IsSet(pconstants.ArgFrom) {
		fromTime, err = parseFromToTime(viper.GetString(pconstants.ArgFrom))
		if err != nil {
			return err
		}
	}
	if viper.IsSet(pconstants.ArgTo) {
		toTime, err = parseFromToTime(viper.GetString(pconstants.ArgTo))
		if err != nil {
			return err
		}
	}
	// validate from and to times
	if err = validateCollectionTImeRange(fromTime, toTime); err != nil {
		return err
	}

	partitions, err := getPartitions(args)
	if err != nil {
		return fmt.Errorf("failed to get partition config: %w", err)
	}

	var partitionNames []string
	for _, partition := range partitions {
		partitionNames = append(partitionNames, partition.FullName)
	}
	slog.Info("Starting collection", "partition(s)", partitionNames, "from", fromTime, "to", toTime)
	// now we have the partitions, we can start collecting

	// start the plugin manager
	pluginManager := plugin.NewPluginManager()
	defer pluginManager.Close()

	// collect each partition serially
	var errList []error
	for _, partition := range partitions {
		// if a from time is set, clear the partition data from that time forward
		if !fromTime.IsZero() && viper.GetBool(pconstants.ArgOverwrite) {
			slog.Info("Deleting parquet files after the from time", "partition", partition.Name, "from", fromTime)
			_, err := parquet.DeleteParquetFiles(partition, fromTime)
			if err != nil {
				slog.Warn("Failed to delete parquet files after the from time", "partition", partition.Name, "from", fromTime, "error", err)
				errList = append(errList, err)
				continue
			}
			slog.Info("Completed deleting parquet files after the from time", "partition", partition.Name, "from", fromTime)
		}
		// do the collection
		err = collectPartition(ctx, cancel, partition, fromTime, toTime, pluginManager)
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

func validateCollectionTImeRange(fromTime time.Time, toTime time.Time) error {
	if !fromTime.IsZero() && !toTime.IsZero() && fromTime.After(toTime) {
		return fmt.Errorf("invalid time range: 'from' time %s is after 'to' time %s", fromTime.Format(time.DateOnly), toTime.Format(time.DateOnly))
	}
	if toTime.After(time.Now()) {
		return fmt.Errorf("invalid time range: 'to' time %s is in the future", toTime.Format(time.DateOnly))
	}
	return nil
}

func collectPartition(ctx context.Context, cancel context.CancelFunc, partition *config.Partition, fromTime time.Time, toTime time.Time, pluginManager *plugin.PluginManager) (err error) {
	c, err := collector.New(pluginManager, partition, cancel)
	if err != nil {
		return fmt.Errorf("failed to create collector: %w", err)
	}
	defer c.Close()

	recollect := viper.GetBool(pconstants.ArgOverwrite)

	if err = c.Collect(ctx, fromTime, toTime, recollect); err != nil {
		return err
	}

	slog.Info("collectPartition - waiting for completion", "partition", partition.Name)
	// now wait for all collection to complete and close the collector
	err = c.WaitForCompletion(ctx)
	if err != nil {
		return err
	}

	slog.Info("Collection complete", "partition", partition.Name)
	// compact the parquet files
	if viper.GetBool(pconstants.ArgCompact) {
		err = c.Compact(ctx)
		if err != nil {
			return err
		}
	}

	// update status to show complete and display collection summary
	c.Completed()

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
	tablePattern, partitionPattern, err := getPartitionMatchPatternsForArg(partitions, arg)
	if err != nil {
		return nil, err
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

func getPartitionMatchPatternsForArg(partitions []string, arg string) (string, string, error) {
	var tablePattern, partitionPattern string
	parts := strings.Split(arg, ".")
	switch len(parts) {
	case 1:
		var err error
		tablePattern, partitionPattern, err = getPartitionMatchPatternsForSinglePartName(partitions, arg)
		if err != nil {
			return "", "", err
		}
	case 2:
		// use the args as provided
		tablePattern = parts[0]
		partitionPattern = parts[1]
	default:
		return "", "", fmt.Errorf("invalid partition name: %s", arg)
	}
	return tablePattern, partitionPattern, nil
}

// getPartitionMatchPatternsForSinglePartName returns the table and partition patterns for a single part name
// e.g. if the arg is "aws*"
func getPartitionMatchPatternsForSinglePartName(partitions []string, arg string) (string, string, error) {
	var tablePattern, partitionPattern string
	// '*' is not valid for a single part arg
	if arg == "*" {
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
	partitionPattern = arg
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

// parse the from time
func parseFromToTime(arg string) (time.Time, error) {
	now := time.Now()

	// validate the granularity
	granularity := time.Hour * 24

	fromTime, err := parse.ParseTime(arg, now)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse '%s' argument: %w", arg, err)
	}

	return fromTime.Truncate(granularity), nil
}
