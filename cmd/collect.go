package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/contexthelpers"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/pipe-fittings/v2/modconfig"
	"github.com/turbot/pipe-fittings/v2/parse"
	localcmdconfig "github.com/turbot/tailpipe/internal/cmdconfig"
	"github.com/turbot/tailpipe/internal/collector"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
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
			if errors.Is(err, context.Canceled) {
				fmt.Println("Collection cancelled.") //nolint:forbidigo // ui output
			} else {
				error_helpers.ShowError(ctx, err)
			}
			setExitCodeForCollectError(&err)
		}
	}()

	// if diagnostic mode is set, print out config and return
	if _, ok := os.LookupEnv(constants.EnvConfigDump); ok {
		localcmdconfig.DisplayConfig()
		return
	}

	err = doCollect(ctx, cancel, args)

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
	if err = validateCollectionTimeRange(fromTime, toTime); err != nil {
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

func validateCollectionTimeRange(fromTime time.Time, toTime time.Time) error {
	if !fromTime.IsZero() && !toTime.IsZero() && fromTime.After(toTime) {
		return fmt.Errorf("invalid time range: 'from' time %s is after 'to' time %s", fromTime.Format(time.DateOnly), toTime.Format(time.DateOnly))
	}
	if toTime.After(time.Now()) {
		return fmt.Errorf("invalid time range: 'to' time %s is in the future", toTime.Format(time.DateOnly))
	}
	return nil
}

func collectPartition(ctx context.Context, cancel context.CancelFunc, partition *config.Partition, fromTime time.Time, toTime time.Time, pluginManager *plugin.PluginManager) (err error) {
	t := time.Now()
	c, err := collector.New(pluginManager, partition, cancel)
	if err != nil {
		return fmt.Errorf("failed to create collector: %w", err)
	}
	defer c.Close()

	overwrite := viper.GetBool(pconstants.ArgOverwrite)

	if err = c.Collect(ctx, fromTime, toTime, overwrite); err != nil {
		return err
	}

	slog.Info("collectPartition - waiting for completion", "partition", partition.Name)
	// now wait for all collection to complete and close the collector
	err = c.WaitForCompletion(ctx)
	if err != nil {
		return err
	}

	slog.Info("Collection complete", "partition", partition.Name, "duration", time.Since(t).Seconds())
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

// getPartitions resolves the provided args to a list of partitions.
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
		if syntheticPartition, ok := getSyntheticPartition(arg); ok {
			partitions = append(partitions, syntheticPartition)
			continue
		}

		partitionNames, err := database.GetPartitionsForArg(tailpipeConfig.Partitions, arg)
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
		// TODO #errors better formating/error message https://github.com/turbot/tailpipe/issues/497
		return nil, errors.Join(errorList...)
	}

	return partitions, nil
}

// getSyntheticPartition parses a synthetic partition specification string and creates a test partition configuration.
// This function enables testing and performance benchmarking by generating dummy data instead of collecting from real sources.
//
// Synthetic partition format: synthetic_<cols>cols_<rows>rows_<chunk>chunk_<interval>ms
// Example: "synthetic_50cols_2000000rows_10000chunk_100ms"
//   - 50cols: Number of columns to generate in the synthetic table
//   - 2000000rows: Total number of rows to generate
//   - 10000chunk: Number of rows per chunk (affects memory usage and processing)
//   - 100ms: Delivery interval between chunks (simulates real-time data collection)
//
// The function validates the format and numeric values, returning a properly configured Partition
// with SyntheticMetadata that will be used by the collector to generate test data.
//
// Returns:
//   - *config.Partition: The configured synthetic partition if parsing succeeds
//   - bool: true if the argument was a valid synthetic partition, false otherwise
func getSyntheticPartition(arg string) (*config.Partition, bool) {
	// Check if this is a synthetic partition by looking for the "synthetic_" prefix
	if !strings.HasPrefix(arg, "synthetic_") {
		return nil, false
	}

	// Parse the synthetic partition parameters by splitting on underscores
	// Expected format: synthetic_<cols>cols_<rows>rows_<chunk>chunk_<interval>ms
	parts := strings.Split(arg, "_")
	if len(parts) != 5 {
		// Invalid format - synthetic partitions must have exactly 5 parts
		slog.Debug("Synthetic partition parsing failed: invalid format", "arg", arg, "parts", len(parts), "expected", 5)
		return nil, false
	}

	// Extract and parse the numeric values from each part
	// Remove the suffix to get just the numeric value
	colsStr := strings.TrimSuffix(parts[1], "cols")
	rowsStr := strings.TrimSuffix(parts[2], "rows")
	chunkStr := strings.TrimSuffix(parts[3], "chunk")
	intervalStr := strings.TrimSuffix(parts[4], "ms")

	// Parse columns count - determines how many columns the synthetic table will have
	cols, err := strconv.Atoi(colsStr)
	if err != nil {
		// Invalid columns value, not a synthetic partition
		slog.Debug("Synthetic partition parsing failed: invalid columns value", "arg", arg, "colsStr", colsStr, "error", err)
		return nil, false
	}

	// Parse rows count - total number of rows to generate
	rows, err := strconv.Atoi(rowsStr)
	if err != nil {
		// Invalid rows value, not a synthetic partition
		slog.Debug("Synthetic partition parsing failed: invalid rows value", "arg", arg, "rowsStr", rowsStr, "error", err)
		return nil, false
	}

	// Parse chunk size - number of rows per chunk (affects memory usage and processing efficiency)
	chunk, err := strconv.Atoi(chunkStr)
	if err != nil {
		// Invalid chunk value, not a synthetic partition
		slog.Debug("Synthetic partition parsing failed: invalid chunk value", "arg", arg, "chunkStr", chunkStr, "error", err)
		return nil, false
	}

	// Parse delivery interval - milliseconds between chunk deliveries (simulates real-time data flow)
	interval, err := strconv.Atoi(intervalStr)
	if err != nil {
		// Invalid interval value, not a synthetic partition
		slog.Debug("Synthetic partition parsing failed: invalid interval value", "arg", arg, "intervalStr", intervalStr, "error", err)
		return nil, false
	}

	// Validate the parsed values - all must be positive integers
	if cols <= 0 || rows <= 0 || chunk <= 0 || interval <= 0 {
		// Invalid values, not a synthetic partition
		slog.Debug("Synthetic partition parsing failed: invalid values", "arg", arg, "cols", cols, "rows", rows, "chunk", chunk, "interval", interval)
		return nil, false
	}

	// Create a synthetic partition with proper HCL block structure
	// This mimics the structure that would be created from a real HCL configuration file
	block := &hcl.Block{
		Type:   "partition",
		Labels: []string{"synthetic", arg},
	}

	// Create the partition configuration with synthetic metadata
	partition := &config.Partition{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fmt.Sprintf("partition.synthetic.%s", arg)),
		TableName:       "synthetic", // All synthetic partitions use the "synthetic" table name
		TpIndexColumn:   "'default'", // Use a default index column for synthetic data
		SyntheticMetadata: &config.SyntheticMetadata{
			Columns:            cols,     // Number of columns to generate
			Rows:               rows,     // Total number of rows to generate
			ChunkSize:          chunk,    // Rows per chunk
			DeliveryIntervalMs: interval, // Milliseconds between chunk deliveries
		},
	}

	// Set the unqualified name for the partition (used in logging and identification)
	partition.UnqualifiedName = fmt.Sprintf("%s.%s", partition.TableName, partition.ShortName)

	slog.Debug("Synthetic partition parsed successfully", "arg", arg, "columns", cols, "rows", rows, "chunkSize", chunk, "deliveryIntervalMs", interval)
	return partition, true
}

func setExitCodeForCollectError(err *error) {
	// if exit code already set, leave as is
	if exitCode != 0 || err == nil || *err == nil {
		return
	}
	// set exit code for cancellation
	if errors.Is(*err, context.Canceled) {
		exitCode = pconstants.ExitCodeOperationCancelled
		// clear the caller's error so we don't surface it as a failure
		*err = nil
		return
	}

	exitCode = pconstants.ExitCodeCollectionFailed
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
