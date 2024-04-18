package tailpipe

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectionInit(t *testing.T) {
	ctx := context.Background()
	InitConfig(ctx, WithMock())
	c, err := NewCollection(ctx, "mock", "foo", "mock")
	require.NoError(t, err)
	assert.Equal(t, "mock", c.Plugin)
	assert.Equal(t, "mock", c.Source)
	assert.Equal(t, "foo", c.Name)
}
