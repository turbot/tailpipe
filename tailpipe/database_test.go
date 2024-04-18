package tailpipe

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseInit(t *testing.T) {
	ctx := context.Background()
	db, err := NewDatabase(ctx)
	require.NoError(t, err)
	assert.NotNil(t, db)
}

func TestDatabaseInitWithPath(t *testing.T) {
	ctx := context.Background()
	p := "./test.db"
	db, err := NewDatabase(ctx, WithPath(p))
	require.NoError(t, err)
	assert.NotNil(t, db)
	assert.Equal(t, p, db.Path)
}

func TestListSchemasEmpty(t *testing.T) {
	ctx := context.Background()
	db, err := NewDatabase(ctx)
	require.NoError(t, err)
	db.Open()
	defer db.Close()
	schemas, err := db.listSchemas()
	require.NoError(t, err)
	assert.Len(t, schemas, 0)
}

func TestCreateSchema(t *testing.T) {
	ctx := context.Background()
	s := &Schema{Name: "test"}
	db, err := NewDatabase(ctx)
	require.NoError(t, err)
	db.Open()
	defer db.Close()
	require.NoError(t, db.createSchema(s))
	schemas, err := db.listSchemas()
	require.NoError(t, err)
	assert.Contains(t, schemas, s.Name)
}

func TestDropSchema(t *testing.T) {
	ctx := context.Background()
	s := &Schema{Name: "test"}
	db, err := NewDatabase(ctx)
	require.NoError(t, err)
	db.Open()
	defer db.Close()
	// Create a schema so we can drop it
	require.NoError(t, db.createSchema(s))
	schemas, err := db.listSchemas()
	require.NoError(t, err)
	assert.Contains(t, schemas, s.Name)
	require.NoError(t, db.dropSchemaCascade(s))
	// Drop the schema
	schemas, err = db.listSchemas()
	require.NoError(t, err)
	assert.NotContains(t, schemas, s.Name)
}

func TestSyncSchemasZeroToTwo(t *testing.T) {
	ctx := context.Background()
	foo := &Schema{Name: "foo"}
	bar := &Schema{Name: "bar"}
	db, err := NewDatabase(ctx, WithSchemas(foo, bar))
	require.NoError(t, err)
	db.Open()
	defer db.Close()
	// Ensure empty to start
	schemas, err := db.listSchemas()
	require.NoError(t, err)
	assert.Empty(t, schemas)
	// Sync
	require.NoError(t, db.SyncSchemas())
	// Ensure both schemas are present
	schemas, err = db.listSchemas()
	require.NoError(t, err)
	assert.ElementsMatch(t, schemas, []string{foo.Name, bar.Name})
}

func TestSyncSchemasTwoToZero(t *testing.T) {
	ctx := context.Background()
	foo := &Schema{Name: "foo"}
	bar := &Schema{Name: "bar"}
	db, err := NewDatabase(ctx, WithSchemas(foo, bar))
	require.NoError(t, err)
	db.Open()
	defer db.Close()
	// Sync to setup the two
	require.NoError(t, db.SyncSchemas())
	// Ensure both schemas are present
	schemas, err := db.listSchemas()
	require.NoError(t, err)
	assert.ElementsMatch(t, schemas, []string{foo.Name, bar.Name})
	// Sync to empty
	db.Schemas = []*Schema{}
	require.NoError(t, db.SyncSchemas())
	// Ensure empty
	schemas, err = db.listSchemas()
	require.NoError(t, err)
	assert.Empty(t, schemas)
}

func TestSyncSchemasOneToTwo(t *testing.T) {
	ctx := context.Background()
	foo := &Schema{Name: "foo"}
	bar := &Schema{Name: "bar"}
	db, err := NewDatabase(ctx, WithSchemas(foo))
	require.NoError(t, err)
	db.Open()
	defer db.Close()
	// Sync to setup the one
	require.NoError(t, db.SyncSchemas())
	schemas, err := db.listSchemas()
	require.NoError(t, err)
	assert.ElementsMatch(t, schemas, []string{foo.Name})
	// Add one and sync
	db.Schemas = append(db.Schemas, bar)
	require.NoError(t, db.SyncSchemas())
	// Ensure results
	schemas, err = db.listSchemas()
	require.NoError(t, err)
	assert.ElementsMatch(t, schemas, []string{foo.Name, bar.Name})
}

func TestSyncSchemasTwoToOne(t *testing.T) {
	ctx := context.Background()
	foo := &Schema{Name: "foo"}
	bar := &Schema{Name: "bar"}
	db, err := NewDatabase(ctx, WithSchemas(foo, bar))
	require.NoError(t, err)
	db.Open()
	defer db.Close()
	// Sync to setup the two
	require.NoError(t, db.SyncSchemas())
	// Ensure both schemas are present
	schemas, err := db.listSchemas()
	require.NoError(t, err)
	assert.ElementsMatch(t, schemas, []string{foo.Name, bar.Name})
	// Remove one and sync
	db.Schemas = db.Schemas[:1]
	require.NoError(t, db.SyncSchemas())
	// Ensure correct schema is present
	schemas, err = db.listSchemas()
	require.NoError(t, err)
	assert.ElementsMatch(t, schemas, []string{foo.Name})
}

func TestSyncSchemasSwapOne(t *testing.T) {
	ctx := context.Background()
	foo := &Schema{Name: "foo"}
	bar := &Schema{Name: "bar"}
	db, err := NewDatabase(ctx, WithSchemas(foo))
	require.NoError(t, err)
	db.Open()
	defer db.Close()
	// Sync to setup the one
	require.NoError(t, db.SyncSchemas())
	schemas, err := db.listSchemas()
	require.NoError(t, err)
	assert.ElementsMatch(t, schemas, []string{foo.Name})
	// Swap and sync
	db.Schemas = []*Schema{bar}
	require.NoError(t, db.SyncSchemas())
	// Ensure results
	schemas, err = db.listSchemas()
	require.NoError(t, err)
	assert.ElementsMatch(t, schemas, []string{bar.Name})
}

func TestSyncSchemasNoChanges(t *testing.T) {
	ctx := context.Background()
	foo := &Schema{Name: "foo"}
	bar := &Schema{Name: "bar"}
	db, err := NewDatabase(ctx, WithSchemas(foo, bar))
	require.NoError(t, err)
	db.Open()
	defer db.Close()
	// Sync to setup the one
	require.NoError(t, db.SyncSchemas())
	schemas, err := db.listSchemas()
	require.NoError(t, err)
	assert.ElementsMatch(t, schemas, []string{foo.Name, bar.Name})
	// Change order and sync - even though there should be no DB change
	db.Schemas = []*Schema{bar, foo}
	require.NoError(t, db.SyncSchemas())
	// Ensure results
	schemas, err = db.listSchemas()
	require.NoError(t, err)
	assert.ElementsMatch(t, schemas, []string{foo.Name, bar.Name})
}

func TestSync(t *testing.T) {
	ctx := context.Background()
	foo, err := NewSchema(ctx, "foo")
	foo.Tables = []string{"aws_cloudtrail"}
	require.NoError(t, err)
	db, err := NewDatabase(ctx, WithSchemas(foo))
	require.NoError(t, err)
	db.Open()
	defer db.Close()
	// Sync to setup the one
	require.NoError(t, db.Sync())
	schemas, err := db.listSchemas()
	require.NoError(t, err)
	assert.ElementsMatch(t, schemas, []string{foo.Name})
	views, err := db.listViews(foo)
	require.NoError(t, err)
	assert.ElementsMatch(t, views, []string{"aws_cloudtrail"})
}
