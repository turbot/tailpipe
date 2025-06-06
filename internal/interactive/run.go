package interactive

import (
	"context"

	"github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/tailpipe/internal/database"
)

// RunInteractivePrompt starts the interactive query prompt
func RunInteractivePrompt(ctx context.Context, db *database.DuckDb) error {
	interactiveClient, err := newInteractiveClient(ctx, db)
	if err != nil {
		error_helpers.ShowErrorWithMessage(ctx, err, "interactive client failed to initialize")
		return err
	}

	// start the interactive prompt in a go routine
	interactiveClient.InteractivePrompt(ctx)

	return nil
}
