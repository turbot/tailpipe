package queryexecute

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/viper"
	"github.com/turbot/pipe-fittings/cmdconfig"
	"github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/pipe-fittings/querydisplay"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe/internal/interactive"
)

func RunInteractiveSession(ctx context.Context) error {
	utils.LogTime("execute.RunInteractiveSession start")
	defer utils.LogTime("execute.RunInteractiveSession end")

	// the db executor sends result data over resultsStreamer
	result := interactive.RunInteractivePrompt(ctx)

	// print the data as it comes
	for r := range result.Streamer.Results {
		querydisplay.ShowOutput(ctx, r)
		// signal to the resultStreamer that we are done with this chunk of the stream
		result.Streamer.AllResultsRead()
	}
	return result.PromptErr
}

func RunBatchSession(ctx context.Context, queries []*modconfig.ResolvedQuery) int {
	// TODO #cancelallation
	// start cancel handler to intercept interrupts and cancel the context
	// NOTE: use the initData Cancel function to ensure any initialisation is cancelled if needed
	//contexthelpers.StartCancelHandler(initData.Cancel)

	utils.LogTime("queryexecute.executeQueries start")
	defer utils.LogTime("queryexecute.executeQueries end")

	// failures return the number of queries that failed and also the number of rows that
	// returned errors
	failures := 0
	t := time.Now()

	var err error

	for i, q := range queries {
		// if executeQuery fails it returns err, else it returns the number of rows that returned errors while execution
		if err, failures = executeQuery(ctx, q); err != nil {
			failures++
			error_helpers.ShowWarning(fmt.Sprintf("query %d of %d failed: %v", i+1, len(queries), error_helpers.DecodePgError(err)))
			// if timing flag is enabled, show the time taken for the query to fail
			if cmdconfig.Viper().GetString(constants.ArgTiming) != constants.ArgOff {
				querydisplay.DisplayErrorTiming(t)
			}
		}
		// TODO move into display layer
		// Only show the blank line between queries, not after the last one
		if (i < len(queries)-1) && showBlankLineBetweenResults() {
			fmt.Println()
		}
	}

	return failures
}

func executeQuery(ctx context.Context, resolvedQuery *modconfig.ResolvedQuery) (error, int) {
	utils.LogTime("query.execute.executeQuery start")
	defer utils.LogTime("query.execute.executeQuery end")

	// TODO #query implement
	//// the db executor sends result data over resultsStreamer
	//resultsStreamer, err := db_common.ExecuteQuery(ctx, client, resolvedQuery.ExecuteSQL, resolvedQuery.Args...)
	//if err != nil {
	//	return err, 0
	//}

	rowErrors := 0 // get the number of rows that returned an error
	// print the data as it comes
	//for r := range resultsStreamer.Results {
	//	rowErrors = querydisplay.ShowOutput(ctx, r)
	//	// signal to the resultStreamer that we are done with this result
	//	resultsStreamer.AllResultsRead()
	//}
	return nil, rowErrors
}

// if we are displaying csv with no header, do not include lines between the query results
func showBlankLineBetweenResults() bool {
	return !(viper.GetString(constants.ArgOutput) == "csv" && !viper.GetBool(constants.ArgHeader))
}
