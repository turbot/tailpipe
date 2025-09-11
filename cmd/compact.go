package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/briandowns/spinner"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/contexthelpers"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	localcmdconfig "github.com/turbot/tailpipe/internal/cmdconfig"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
	"golang.org/x/exp/maps"
)

// TODO #DL update docs - no longer support compacting single partition
//
//	https://github.com/turbot/tailpipe/issues/474
func compactCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compact [table|table.partition] [flags]",
		Args:  cobra.MaximumNArgs(1),
		Run:   runCompactCmd,
		Short: "Compact multiple parquet files per day to one per day",
		Long:  `Compact multiple parquet files per day to one per day.`,
	}

	cmdconfig.OnCmd(cmd).
		AddBoolFlag(pconstants.ArgReindex, false, "Update the tp_index field to the currently configured value.")
	return cmd
}

func runCompactCmd(cmd *cobra.Command, args []string) {
	var err error
	ctx, cancel := context.WithCancel(cmd.Context())
	contexthelpers.StartCancelHandler(cancel)

	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
		if err != nil {
			setExitCodeForCompactError(err)

			if errors.Is(err, context.Canceled) {
				//nolint:forbidigo // ui
				fmt.Println("Compact cancelled")
			} else {
				error_helpers.ShowError(ctx, err)
			}
		}
	}()

	// if diagnostic mode is set, print out config and return
	if _, ok := os.LookupEnv(constants.EnvConfigDump); ok {
		localcmdconfig.DisplayConfig()
		return
	}

	slog.Info("Compacting parquet files")

	db, err := database.NewDuckDb(database.WithDuckLake())
	error_helpers.FailOnError(err)
	defer db.Close()

	// verify that the provided args resolve to at least one partition
	if _, err := getPartitions(args); err != nil {
		error_helpers.FailOnError(err)
	}

	// Get table and partition patterns
	patterns, err := database.GetPartitionPatternsForArgs(maps.Keys(config.GlobalConfig.Partitions), args...)
	error_helpers.FailOnErrorWithMessage(err, "failed to get partition patterns")

	// do the compaction

	status, err := doCompaction(ctx, db, patterns)
	// print the final status
	statusString := status.VerboseString()
	if err == nil {
		fmt.Println(statusString) //nolint:forbidigo // ui
	}

	// defer block will show the error
}

func doCompaction(ctx context.Context, db *database.DuckDb, patterns []*database.PartitionPattern) (*database.CompactionStatus, error) {
	s := spinner.New(
		spinner.CharSets[14],
		100*time.Millisecond,
		spinner.WithHiddenCursor(true),
		spinner.WithWriter(os.Stdout),
	)
	// if the flag was provided, migrate the tp_index files
	reindex := viper.GetBool(pconstants.ArgReindex)

	// start and stop spinner around the processing
	s.Start()
	defer s.Stop()
	s.Suffix = " compacting parquet files"
	// define func to update the spinner suffix with the number of files compacted
	var status = database.NewCompactionStatus()

	updateTotals := func(updatedStatus database.CompactionStatus) {
		status = &updatedStatus
		if status.Message != "" {
			s.Suffix = " compacting parquet files: " + status.Message
		}
	}

	// do compaction
	err := database.CompactDataFiles(ctx, db, updateTotals, reindex, patterns...)

	return status, err
}

func setExitCodeForCompactError(err error) {
	// set exit code only if an error occurred and no exit code is already set
	if exitCode != 0 || err == nil {
		return
	}
	// set exit code for cancellation
	if errors.Is(err, context.Canceled) {
		exitCode = pconstants.ExitCodeOperationCancelled
		return
	}

	exitCode = pconstants.ExitCodeCompactFailed
}
