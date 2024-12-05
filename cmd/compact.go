package cmd

import (
	"context"
	"fmt"
	"github.com/briandowns/spinner"
	"github.com/spf13/cobra"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/cmdconfig"
	"github.com/turbot/pipe-fittings/contexthelpers"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/tailpipe/internal/parquet"
	"os"
	"time"
)

func compactCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compact [flags]",
		Args:  cobra.ArbitraryArgs,
		Run:   runCompactCmd,
		Short: "compact the data files",
		Long:  `compact the parquet data files into one file per day.`,
	}

	cmdconfig.OnCmd(cmd)
	return cmd
}

func runCompactCmd(cmd *cobra.Command, _ []string) {
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

	var statusString string
	statusString, err = doCompaction(ctx)
	if err == nil {
		fmt.Println(statusString) //nolint:forbidigo // ui
		fmt.Println(statusString) //nolint:forbidigo // ui
	}

	// if there was an error, defer block will show
}

func doCompaction(ctx context.Context) (string, error) {
	s := spinner.New(
		spinner.CharSets[14],
		100*time.Millisecond,
		spinner.WithHiddenCursor(true),
		spinner.WithWriter(os.Stdout),
	)

	// start and stop spinner around the processing

	defer s.Stop()
	s.Suffix = " compacting parquet files"

	// define func to update the spinner suffix with the number of files compacted
	var total parquet.CompactionCounts
	updateTotals := func(counts parquet.CompactionCounts) {
		total.Update(counts)
		s.Suffix = fmt.Sprintf(" compacting parquet files (%d files -> %d files)", total.Source, total.Dest)
	}

	// do compaction
	s.Start()
	err := parquet.CompactDataFiles(ctx, updateTotals)
	s.Stop()

	if err != nil && ctx.Err() == nil {
		return "", err
	}

	// print the final status
	statusString := total.String()
	if statusString == "" {
		statusString = "No files to compact."
	}
	if ctx.Err() != nil {
		statusString = "Compaction cancelled: " + statusString
	}
	return statusString, nil
}

func setExitCodeForCompactError(err error) {
	// set exit code only if an error occurred and no exit code is already set
	if exitCode != 0 || err == nil {
		return
	}
	exitCode = 1
}
