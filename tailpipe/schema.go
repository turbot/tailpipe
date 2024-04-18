package tailpipe

import (
	"context"
	"errors"
	"sync"

	"github.com/turbot/tailpipe/types"

	"github.com/turbot/tailpipe-plugin-sdk/observer"
)

type Schema struct {
	Name string `json:"name"`

	// Collections is a list of collection names that should be included
	// in this schema. The list may include wildcards and is matched against
	// the full set of collections defined in the configuration.
	Collections []string `json:"collections"`

	// Connections is a list of connection names that should be included
	// in this schema. The list may include wildcards and is matched against
	// the full set of connections discovered in the collection(s).
	Connections []string `json:"connections"`

	// Tables is a list of table names that should be included in this schema.
	// The list may include wildcards and is matched against the full set of
	// tables discovered in the collection(s).
	Tables []string `json:"tables"`

	ctx                context.Context
	database           types.Database
	matchedCollections []*Collection

	// observers is a list of observers that will be notified of events.
	observers      []observer.ObserverInterface
	observersMutex sync.RWMutex
}

type SchemaOption func(*Schema) error

func NewSchema(ctx context.Context, name string, opts ...SchemaOption) (*Schema, error) {
	s := &Schema{
		Name: name,
	}
	if err := s.Init(ctx, opts...); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Schema) Init(ctx context.Context, opts ...SchemaOption) error {

	s.ctx = ctx

	for _, opt := range opts {
		if err := opt(s); err != nil {
			return err
		}
	}

	if s.Collections == nil {
		s.Collections = []string{"*"}
	}
	if s.Connections == nil {
		s.Connections = []string{"*"}
	}
	if s.Tables == nil {
		s.Tables = []string{"*"}
	}
	if s.matchedCollections == nil {
		s.matchedCollections = []*Collection{}
	}

	return s.Validate()
}

func (s *Schema) Validate() error {
	if s.Name == "" {
		return errors.New("schema name is required")
	}
	return nil
}

func WithSchemaObservers(observers ...observer.ObserverInterface) SchemaOption {
	return func(s *Schema) error {
		s.observers = append(s.observers, observers...)
		return nil
	}
}

func (s *Schema) AddObserver(o observer.ObserverInterface) {
	s.observersMutex.Lock()
	defer s.observersMutex.Unlock()
	s.observers = append(s.observers, o)
}

func (s *Schema) NotifyObservers(event observer.Event) {
	s.observersMutex.RLock()
	defer s.observersMutex.RUnlock()
	for _, o := range s.observers {
		o.Notify(event)
	}
}

func (s *Schema) SetDatabase(db types.Database) {
	s.database = db
}

func (s *Schema) Database() types.Database {
	return s.database
}

func (s *Schema) Identifier() string {
	return s.Name
}

func (s *Schema) CreateStatement() string {
	return "create schema " + s.Identifier()
}

func (s *Schema) DropCascadeStatement() string {
	return "drop schema " + s.Identifier() + " cascade"
}

func (s *Schema) Views() (map[string]*View, error) {
	views := map[string]*View{}
	for _, table := range s.Tables {
		tmp, err := NewView(s.ctx, table, WithSchema(s))
		if err != nil {
			return nil, err
		}
		tmp.Tables = s.Tables
		tmp.Collections = s.Collections
		tmp.Connections = s.Connections
		views[table] = tmp
	}
	return views, nil
}

/*
func (s *Schema) Views() []string {
	conf := config.GetConfig()
	hiveNames := []string{}
	for _, coll := range conf.CollectionsMatching(s.Collections) {
		for _, conn := range s.Connections {
			hiveNames = append(hiveNames, fmt.Sprintf("tp_collection=%s/tp_connection=%s/tp_year=* /tp_month=* /tp_day=*", coll.Identifier(), conn))
		}
	}
	viewStatements := []string{}
	for _, table := range s.Tables {
		viewStatements = append(viewStatements, fmt.Sprintf(`create view %s.%s as select * from read_parquet(['%s'])`, s.Identifier(), table, strings.Join(hiveNames, "','")))
	}
	return viewStatements
}
*/

/*
func (s *Schema) Views() []*View {
	var views []*View
	for _, c := range s.Collections {
		tmp := fmt.Sprintf("create view %s.%s as select * from read_parquet($1)", s.Identifier(), c.Plugin().Identifier())
		views = append(views, c.Views()...)
	}
	return views
}
*/

/*
func (s *Schema) Views() []*View {
	// TODO - this is a temporary implementation
	tmp, err := NewView(s.ctx, "aws_cloudtrail", WithSchema(s))
	if err != nil {
		return nil
	}
	return []*View{tmp}
}
*/
