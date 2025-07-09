package cmd

import (
	"context"
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"log/slog"
	"os"
	"time"

	"github.com/briandowns/spinner"
	"github.com/spf13/cobra"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/contexthelpers"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	localcmdconfig "github.com/turbot/tailpipe/internal/cmdconfig"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/parquet"
)

func compactCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compact [table|table.partition] [flags]",
		Args:  cobra.ExactArgs(0),
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
			error_helpers.ShowError(ctx, err)
		}
	}()

	// if diagnostic mode is set, print out config and return
	if _, ok := os.LookupEnv(constants.EnvConfigDump); ok {
		localcmdconfig.DisplayConfig()
		return
	}

	slog.Info("Compacting parquet files")

	// if the flag was provided, migrate the tp_index files
	if viper.GetBool(pconstants.ArgReindex) {
		// TODO #DL look at migration
		panic("Reindexing is not yet implemented for ducklake")
	}

	db, err := database.NewDuckDb(database.WithDuckLakeEnabled(true))
	error_helpers.FailOnError(err)
	defer db.Close()

	// do the compaction
	status, err := doCompaction(ctx, db)
	if errors.Is(err, context.Canceled) {
		// clear error so we don't show it with normal error reporting
		err = nil
	}

	if err == nil {
		// print the final status
		statusString := status.VerboseString()
		if statusString == "" {
			statusString = "No files to compact."
		}
		if ctx.Err() != nil {
			// instead show the status as cancelled
			statusString = "Compaction cancelled: " + statusString
		}

		fmt.Println(statusString) //nolint:forbidigo // ui
	}

	// defer block will show the error
}

func doCompaction(ctx context.Context, db *database.DuckDb) (*parquet.CompactionStatus, error) {
	s := spinner.New(
		spinner.CharSets[14],
		100*time.Millisecond,
		spinner.WithHiddenCursor(true),
		spinner.WithWriter(os.Stdout),
	)

	// start and stop spinner around the processing
	s.Start()
	defer s.Stop()
	s.Suffix = " compacting parquet files"

	// do compaction
	status, err := parquet.CompactDataFiles(ctx, db)

	s.Suffix = fmt.Sprintf(" compacted parquet files (%d files -> %d files)", status.Source, status.Dest)

	return status, err
}

func setExitCodeForCompactError(err error) {
	// set exit code only if an error occurred and no exit code is already set
	if exitCode != 0 || err == nil {
		return
	}
	exitCode = 1
}
