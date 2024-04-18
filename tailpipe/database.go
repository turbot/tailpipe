package tailpipe

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/turbot/tailpipe-plugin-sdk/observer"
)

type DuckDBDatabase struct {
	Name    string
	Schemas []*Schema

	// Path is the path to the database file. If empty, which is the default,
	// the database will be in-memory.
	Path string

	ctx  context.Context
	conn *sql.DB

	// observers is a list of observers that will be notified of events.
	observers      []observer.ObserverInterface
	observersMutex sync.RWMutex
}

type DatabaseOption func(*DuckDBDatabase) error

func NewDatabase(ctx context.Context, opts ...DatabaseOption) (*DuckDBDatabase, error) {

	db := &DuckDBDatabase{
		Schemas: []*Schema{},

		ctx: ctx,
	}

	for _, opt := range opts {
		if err := opt(db); err != nil {
			return nil, err
		}
	}

	return db, nil
}

func WithPath(path string) DatabaseOption {
	return func(db *DuckDBDatabase) error {
		return db.setPath(path)
	}
}

func WithSchemas(schemas ...*Schema) DatabaseOption {
	return func(db *DuckDBDatabase) error {
		for _, s := range schemas {
			s.SetDatabase(db)
			db.Schemas = append(db.Schemas, s)
		}
		return nil
	}
}

func WithDatabaseObservers(observers ...observer.ObserverInterface) DatabaseOption {
	return func(db *DuckDBDatabase) error {
		db.observers = append(db.observers, observers...)
		return nil
	}
}

func (db *DuckDBDatabase) setPath(path string) error {
	db.Path = path
	return nil
}

func (db *DuckDBDatabase) AddObserver(o observer.ObserverInterface) {
	db.observersMutex.Lock()
	defer db.observersMutex.Unlock()
	db.observers = append(db.observers, o)
}

func (db *DuckDBDatabase) NotifyObservers(event observer.Event) {
	db.observersMutex.RLock()
	defer db.observersMutex.RUnlock()
	for _, o := range db.observers {
		o.Notify(event)
	}
}

func (db *DuckDBDatabase) Open() error {
	if db.conn != nil {
		// Connection is already open
		return nil
	}
	var err error
	if err := os.MkdirAll(filepath.Dir(db.Path), 0755); err != nil {
		return err
	}
	db.conn, err = sql.Open("duckdb", db.Path)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	return nil
}

func (db *DuckDBDatabase) EnsureOpen() error {
	if db.conn == nil {
		return db.Open()
	}
	return nil
}

func (db *DuckDBDatabase) Close() error {
	if db.conn == nil {
		// Connection is already closed
		return nil
	}
	if err := db.conn.Close(); err != nil {
		return fmt.Errorf("failed to close database: %v", err)
	}
	return nil
}

// listSchemas returns a list of schema names from the current catalog of the
// database in alphabetical order.
func (db *DuckDBDatabase) listSchemas() ([]string, error) {
	var schemas []string

	if err := db.EnsureOpen(); err != nil {
		return schemas, err
	}

	query := `
	  select schema_name
	  from information_schemata
	  where schema_name not in ('information_schema', 'main', 'pg_catalog')
	  order by schema_name
	`

	rows, err := db.conn.QueryContext(db.ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		schemas = append(schemas, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return schemas, nil
}

func (db *DuckDBDatabase) listViews(s *Schema) ([]string, error) {
	var views []string

	if err := db.EnsureOpen(); err != nil {
		return views, err
	}

	query := `
	  select table_name
	  from information_tables
	  where table_schema = $1 and table_type = 'VIEW'
	  order by table_name
	`

	rows, err := db.conn.QueryContext(db.ctx, query, s.Name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		views = append(views, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return views, nil
}

func (db *DuckDBDatabase) createSchema(s *Schema) error {
	if err := db.EnsureOpen(); err != nil {
		return err
	}
	_, err := db.conn.ExecContext(db.ctx, s.CreateStatement())
	return err
}

func (db *DuckDBDatabase) dropSchemaCascade(s *Schema) error {
	if err := db.EnsureOpen(); err != nil {
		return err
	}
	_, err := db.conn.ExecContext(db.ctx, s.DropCascadeStatement())
	return err
}

func (db *DuckDBDatabase) SyncSchemas() error {
	var currentSchemas, targetSchemas []string
	for _, s := range db.Schemas {
		targetSchemas = append(targetSchemas, s.Name)
	}
	currentSchemas, err := db.listSchemas()
	if err != nil {
		return err
	}
	schemasToCreate := difference(targetSchemas, currentSchemas)
	for _, schemaName := range schemasToCreate {
		s := &Schema{Name: schemaName}
		if err := db.createSchema(s); err != nil {
			return err
		}
	}
	schemasToDrop := difference(currentSchemas, targetSchemas)
	for _, schemaName := range schemasToDrop {
		s := &Schema{Name: schemaName}
		if err := db.dropSchemaCascade(s); err != nil {
			return err
		}
	}
	return nil
}

func (db *DuckDBDatabase) createView(v *View) error {
	if err := db.EnsureOpen(); err != nil {
		return err
	}
	_, err := db.conn.ExecContext(db.ctx, v.CreateStatement())
	return err
}

func (db *DuckDBDatabase) dropView(v *View) error {
	if err := db.EnsureOpen(); err != nil {
		return err
	}
	_, err := db.conn.ExecContext(db.ctx, v.DropStatement())
	return err
}

func (db *DuckDBDatabase) SyncViews(s *Schema) error {
	var currentViews, targetViews []string
	targetViewObjs, err := s.Views()
	if err != nil {
		return err
	}
	for k := range targetViewObjs {
		targetViews = append(targetViews, k)
	}
	currentViews, err = db.listViews(s)
	if err != nil {
		return err
	}
	viewsToCreate := difference(targetViews, currentViews)
	for _, viewName := range viewsToCreate {
		if err := db.createView(targetViewObjs[viewName]); err != nil {
			return err
		}
	}
	viewsToDrop := difference(currentViews, targetViews)
	for _, viewName := range viewsToDrop {
		v, err := NewView(db.ctx, viewName, WithSchema(s))
		if err != nil {
			return err
		}
		if err := db.dropView(v); err != nil {
			return err
		}
	}
	return nil
}

// difference returns a new slice containing the elements in `a` that aren't in `b`.
func difference(a, b []string) []string {
	mb := make(map[string]struct{}, len(b))
	for _, x := range b {
		mb[x] = struct{}{}
	}
	var diff []string
	for _, x := range a {
		if _, found := mb[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}

func (db *DuckDBDatabase) Sync() error {
	if err := db.SyncSchemas(); err != nil {
		return err
	}
	for _, s := range db.Schemas {
		if err := db.SyncViews(s); err != nil {
			return err
		}
	}
	return nil
}
