package interactive

import (
	"context"
)

func (c *InteractiveClient) initialiseSuggestions(ctx context.Context) error {
	// reset suggestions
	c.suggestions = newAutocompleteSuggestions()
	c.suggestions.sort()
	return nil
}
