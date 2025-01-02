package metaquery

import (
	"github.com/c-bata/go-prompt"
)

// HandlerInput defines input data for the metaquery handler
type HandlerInput struct {
	Prompt      *prompt.Prompt
	ClosePrompt func()
	Query       string
}

func (h *HandlerInput) args() []string {
	return getArguments(h.Query)
}
