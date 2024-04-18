package tailpipe

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	_, err := InitConfig(context.Background())
	require.NoError(t, err)
}

func TestGetConfig(t *testing.T) {
	type contextKey string
	ctx := context.WithValue(context.Background(), contextKey("foo"), "bar")
	c1, err := InitConfig(ctx)
	require.NoError(t, err)
	c2 := GetConfig()
	assert.Equal(t, ctx, c1.ctx)
	assert.Equal(t, ctx, c2.ctx)
	assert.Equal(t, c1, c2)
}

func TestLoadBytesSimple(t *testing.T) {
	c, err := InitConfig(context.Background())
	require.NoError(t, err)
	cfg := []byte(`{"destinations": { "foo": {"name": "foo"} } }`)
	require.NoError(t, c.LoadBytes(cfg))
	assert.NotEmpty(t, c.Destinations)
	assert.Equal(t, "foo", c.Destinations["foo"].Name)
}

func TestLoadBytesEmptyObject(t *testing.T) {
	c, err := InitConfig(context.Background())
	require.NoError(t, err)
	cfg := []byte(`{}`)
	require.NoError(t, c.LoadBytes(cfg))
	assert.Contains(t, c.Destinations, "default")
	assert.Empty(t, c.Collections)
	assert.Empty(t, c.Schemas)
	assert.Empty(t, c.Sources)
}

func TestLoadBytesErrorEmptyString(t *testing.T) {
	c, err := InitConfig(context.Background())
	require.NoError(t, err)
	cfg := []byte(``)
	err = c.LoadBytes(cfg)
	assert.ErrorContains(t, err, "unexpected end of JSON input")
}

func TestLoadBytesErrorInvalidJSON(t *testing.T) {
	c, err := InitConfig(context.Background())
	require.NoError(t, err)
	cfg := []byte(`{"destinations": { "foo": {"name" "foo"} } }`)
	err = c.LoadBytes(cfg)
	assert.ErrorContains(t, err, "invalid character")
}

func TestLoadBytesErrorInvalidShape(t *testing.T) {
	c, err := InitConfig(context.Background())
	require.NoError(t, err)
	cfg := []byte(`{"destinations": [ {"name": "foo"} ] }`)
	err = c.LoadBytes(cfg)
	assert.ErrorContains(t, err, "cannot unmarshal array")
}

func TestLoadDestinations(t *testing.T) {
	c, err := InitConfig(context.Background())
	require.NoError(t, err)
	cfg := []byte(`{"destinations": { "foo": {"name": "foo"} } }`)
	require.NoError(t, c.LoadBytes(cfg))
	assert.NotEmpty(t, c.Destinations)
	assert.Equal(t, "foo", c.Destinations["foo"].Name)
}

func TestLoadCollections(t *testing.T) {
	c, err := InitConfig(context.Background(), WithMock())
	require.NoError(t, err)
	cfg := []byte(`{"collections": { "foo": {"name": "foo", "plugin": "mock", "source": "mock"} } }`)
	require.NoError(t, c.LoadBytes(cfg))
	assert.NotEmpty(t, c.Collections)
	assert.Equal(t, "foo", c.Collections["foo"].Name)
	assert.Greater(t, c.Collections["foo"].SyncArtifactTimeout, 0)
}

func TestLoadSchemas(t *testing.T) {
	c, err := InitConfig(context.Background())
	require.NoError(t, err)
	cfg := []byte(`{"schemas": { "foo": {"name": "foo"} } }`)
	require.NoError(t, c.LoadBytes(cfg))
	assert.NotEmpty(t, c.Schemas)
	assert.Equal(t, "foo", c.Schemas["foo"].Name)
	assert.NotEmpty(t, c.Schemas["foo"].Collections)
}
