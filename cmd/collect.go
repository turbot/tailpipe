package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/danwakefield/fnmatch"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/tailpipe/internal/collector"
	"github.com/turbot/tailpipe/internal/config"
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

	cmdconfig.OnCmd(cmd).
		AddBoolFlag(pconstants.ArgCompact, true, "Compact the parquet files after collection")

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
	statusString, timingString, err := doCollect(ctx, args)
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
	fmt.Println(statusString) //nolint:forbidigo // ui output
	if compactStatusString != "" {
		fmt.Println(compactStatusString) //nolint:forbidigo // ui output
	}
	fmt.Println(timingString) //nolint:forbidigo //ui output

	return nil
}

func getPartitionConfig(partitionNames []string) ([]*config.Partition, error) {
	// we have loaded tailpipe config by this time
	tailpipeConfig := config.GlobalConfig

	// if no partitions specified, return all
	if len(partitionNames) == 0 {
		return maps.Values(tailpipeConfig.Partitions), nil
	}

	var errorList []error
	var partitions []*config.Partition

	for _, name := range partitionNames {
		partitionNames, err := getPartition(maps.Keys(tailpipeConfig.Partitions), name)
		if err != nil {
			errorList = append(errorList, err)
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

func doCollect(ctx context.Context, args []string) (string, string, error) {
	partitions, err := getPartitionConfig(args)
	if err != nil {
		return "", "", fmt.Errorf("failed to get partition config: %w", err)

	}
	if len(partitions) == 0 {
		return "", "", fmt.Errorf("no partitions found matching args %s", strings.Join(args, " "))
	}

	// now we have the partitions, we can start collecting

	// create a collector
	c, err := collector.New(ctx)
	if err != nil {
		return "", "", fmt.Errorf("failed to create collector: %w", err)
	}
	// stop the spinner
	defer c.Close()

	var errList []error
	for _, col := range partitions {
		if err := c.Collect(ctx, col); err != nil {
			errList = append(errList, err)
		}
	}
	if len(errList) > 0 {
		err = errors.Join(errList...)
		return "", "", fmt.Errorf("collection error: %w", err)
	}

	// now wait for all partitions to complete and close the collector
	c.WaitForCompletion(ctx)
	return c.StatusString(), c.TimingString(), nil
}

func getPartition(partitions []string, name string) ([]string, error) {
	var tablePattern, partitionPattern string
	parts := strings.Split(name, ".")
	switch len(parts) {
	case 1:
		var err error
		tablePattern, partitionPattern, err = getPartitionMatchPatterns(partitions, name, parts, tablePattern, partitionPattern)
		if err != nil {
			return nil, err
		}
	case 2:
		// use the args as provided
		tablePattern = parts[0]
		partitionPattern = parts[1]
	default:
		return nil, fmt.Errorf("invalid partition name: %s", name)
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

func getPartitionMatchPatterns(partitions []string, name string, parts []string, tablePattern string, partitionPattern string) (string, string, error) { //nolint:staticcheck // TODO is tablePattern required as input?
	// '*' is not valid for a single part arg
	if parts[0] == "*" {
		return "", "", fmt.Errorf("invalid partition name: %s", name)
	}
	// check whether there is table with this name
	// partitions is a list of Unqualified names, i.e. <table>.<partition>
	for _, partition := range partitions {
		table := strings.Split(partition, ".")[0]

		// so there IS a table with this name - set partitionPattern to *
		if table == name {
			tablePattern = name    //nolint:staticcheck // required for this logic
			partitionPattern = "*" //nolint:staticcheck // required for this logic
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
