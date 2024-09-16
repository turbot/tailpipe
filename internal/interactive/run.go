package interactive

import (
	"context"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/tailpipe/internal/queryresult"
)

type RunInteractivePromptResult struct {
	Streamer  *queryresult.ResultStreamer
	PromptErr error
}

// RunInteractivePrompt starts the interactive query prompt
func RunInteractivePrompt(ctx context.Context) *RunInteractivePromptResult {
	res := &RunInteractivePromptResult{
		Streamer: queryresult.NewResultStreamer(),
	}

	interactiveClient, err := newInteractiveClient(ctx, res)
	if err != nil {
		error_helpers.ShowErrorWithMessage(ctx, err, "interactive client failed to initialize")
		res.PromptErr = err
		return res
	}

	// start the interactive prompt in a go routine
	go interactiveClient.InteractivePrompt(ctx)

	return res
}
