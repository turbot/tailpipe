package metaquery

import (
	"context"

	"github.com/c-bata/go-prompt"

	"github.com/turbot/tailpipe/internal/database"
)

// HandlerInput defines input data for the metaquery handler
type HandlerInput struct {
	Prompt      *prompt.Prompt
	ClosePrompt func()
	Query       string

	tables *[]string
	Db     *database.DuckDb
}

func (h *HandlerInput) args() []string {
	return getArguments(h.Query)
}

func (h *HandlerInput) GetTables(ctx context.Context) ([]string, error) {
	if h.tables == nil {
		tables, err := database.GetTables(ctx, h.Db)
		if err != nil {
			return nil, err
		}
		h.tables = &tables
	}
	return *h.tables, nil
}
