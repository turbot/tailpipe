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

	views *[]string
}

func (h *HandlerInput) args() []string {
	return getArguments(h.Query)
}

func (h *HandlerInput) GetViews() ([]string, error) {
	if h.views == nil {
		views, err := database.GetTableViews(context.Background())
		if err != nil {
			return nil, err
		}
		h.views = &views
	}
	return *h.views, nil
}
