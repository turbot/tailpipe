package cmd

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/cmdconfig"
	"github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/tailpipe/internal/collector"
	"github.com/turbot/tailpipe/internal/parse"
)

// NOTE: the hard coded config that was previously defined here has been moved to hcl in the file tailpipe/internal/parse/test_data/configs/resources.tpc
// to reference this use: collect --config-path <path to tailpipe>/internal/parse/test_data/configs --partition aws_cloudtrail_log.cloudtrail_logs

// TODO #errors have good think about error handling and return codes

func collectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "collect [flags]",
		TraverseChildren: true,
		Run:              runCollectCmd,
		Short:            "Collect logs from configured sources",
		Long:             `Collect logs from configured sources.`,
	}

	cmdconfig.OnCmd(cmd).
		AddStringFlag(constants.ArgConfigPath, ".", "Path to search for config files").
		AddStringSliceFlag(constants.ArgPartition, nil, "Partition(s) to collect (default is all)")

	return cmd
}

func runCollectCmd(cmd *cobra.Command, _ []string) {
	ctx := cmd.Context()

	var err error
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
			error_helpers.ShowError(ctx, err)
		}
		setExitCodeForCollectError(err)
	}()

	collectionArgs := viper.GetStringSlice(constants.ArgPartition)
	if len(collectionArgs) == 0 {
		// TODO #error think about error codes
		// TODO think about how to show usage
		error_helpers.FailOnError(fmt.Errorf("no partitions specified"))
	}

	partitions, err := parse.GetPartitionConfig(viper.GetStringSlice(constants.ArgPartition), viper.GetString(constants.ArgConfigPath))
	if err != nil {
		// TODO #errors - think about error codes
		error_helpers.FailOnError(fmt.Errorf("failed to get partition config: %w", err))
	}

	// now we have the partitions, we can start collecting

	// create a collector
	c, err := collector.New(ctx)
	if err != nil {
		error_helpers.FailOnError(fmt.Errorf("failed to create collector: %w", err))
	}

	var errList []error
	for _, col := range partitions {
		if err := c.Collect(ctx, col); err != nil {
			errList = append(errList, err)
		}
	}
	if len(errList) > 0 {
		err = errors.Join(errList...)
		error_helpers.FailOnError(fmt.Errorf("collection error: %w", err))
	}

	// now wait for all partitions to complete and close the collector
	c.Close(ctx)
}

func setExitCodeForCollectError(err error) {
	// if exit code already set, leave as is
	if exitCode != 0 || err == nil {
		return
	}

	// TODO #errors - assign exit codes
	exitCode = 1
}
