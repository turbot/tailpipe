package interactive

import (
	"context"
	"fmt"
)

func RunInteractiveQuery(ctx context.Context) error {
	return fmt.Errorf("not implemented quite yet, watch this space")
	c, err := NewInteractiveClient()
	if err != nil {
		return err
	}
	return c.Run(ctx)
}
