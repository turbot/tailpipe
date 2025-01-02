package interactive

import (
	"context"
	"database/sql"
	"github.com/turbot/pipe-fittings/error_helpers"
)

// RunInteractivePrompt starts the interactive query prompt
func RunInteractivePrompt(ctx context.Context, db *sql.DB) error {
	interactiveClient, err := newInteractiveClient(ctx, db)
	if err != nil {
		error_helpers.ShowErrorWithMessage(ctx, err, "interactive client failed to initialize")
		return err
	}

	// start the interactive prompt in a go routine
	interactiveClient.InteractivePrompt(ctx)

	return nil
}
