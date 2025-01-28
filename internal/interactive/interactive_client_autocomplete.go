package interactive

import (
	"context"
	"log"
)

func (c *InteractiveClient) initialiseSuggestions(ctx context.Context) error {
	log.Printf("[TRACE] initialiseSuggestions")

	// reset suggestions
	c.suggestions = newAutocompleteSuggestions()
	c.suggestions.sort()
	return nil
}
