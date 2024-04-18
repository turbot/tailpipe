package tailpipe

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/observer"
	"github.com/turbot/tailpipe/types"
)

type View struct {
	Name     string
	Database types.Database
	Schema   types.SchemaInterface

	Columns     []string
	Tables      []string
	Collections []string
	Connections []string
	StartDate   time.Time
	EndDate     time.Time

	ctx context.Context

	// observers is a list of observers that will be notified of events.
	observers      []observer.ObserverInterface
	observersMutex sync.RWMutex
}

type ViewOption func(*View) error

func NewView(ctx context.Context, name string, opts ...ViewOption) (*View, error) {
	v := &View{
		Name: name,
	}
	if err := v.Init(ctx, opts...); err != nil {
		return nil, err
	}
	return v, nil
}

func (v *View) Init(ctx context.Context, opts ...ViewOption) error {

	v.ctx = ctx

	for _, opt := range opts {
		if err := opt(v); err != nil {
			return err
		}
	}

	if v.Columns == nil {
		v.Columns = []string{"*"}
	}
	if v.Collections == nil {
		v.Collections = []string{"*"}
	}
	if v.Connections == nil {
		v.Connections = []string{"*"}
	}
	if v.Tables == nil {
		v.Tables = []string{"*"}
	}

	return v.Validate()
}

func (v *View) Validate() error {
	if v.Name == "" {
		return errors.New("view name is required")
	}
	return nil
}

func WithObservers(observers ...observer.ObserverInterface) ViewOption {
	return func(v *View) error {
		v.observers = append(v.observers, observers...)
		return nil
	}
}

func WithSchema(s types.SchemaInterface) ViewOption {
	return func(v *View) error {
		v.Schema = s
		return nil
	}
}

func (v *View) AddObserver(o observer.ObserverInterface) {
	v.observersMutex.Lock()
	defer v.observersMutex.Unlock()
	v.observers = append(v.observers, o)
}

func (v *View) NotifyObservers(event observer.Event) {
	v.observersMutex.RLock()
	defer v.observersMutex.RUnlock()
	for _, o := range v.observers {
		o.Notify(event)
	}
}

func (v *View) SetDatabase(db types.Database) {
	v.Database = db
}

func (v *View) SetSchema(s types.SchemaInterface) {
	v.Schema = s
}

func (v *View) Identifier() string {
	return fmt.Sprintf("%s.%s", v.Schema.Identifier(), v.Name)
}

func (v *View) CreateStatementOld() string {
	return fmt.Sprintf("create view %s as select * from '/Users/nathan/src/play-duckdb/daily/*/*/*/*.parquet'", v.Identifier())
}

func (v *View) CreateStatement() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	hivePaths := []string{}
	for _, table := range v.Tables {
		for _, collection := range v.Collections {
			for _, connection := range v.Connections {
				for _, dateMatch := range v.dateMatchStrings() {
					hivePaths = append(hivePaths, filepath.Join(homeDir, "Downloads", "tailpipe", fmt.Sprintf("tp_table=%s/tp_collection=%s/tp_connection=%s/%s", table, collection, connection, dateMatch), "*.parquet"))
				}
			}
		}
	}
	return fmt.Sprintf("create view %s as select %s from read_parquet(['%s'])", v.Identifier(), strings.Join(v.Columns, ","), strings.Join(hivePaths, "','"))
}

func (v *View) DropStatement() string {
	return fmt.Sprintf("drop view %s", v.Identifier())
}

func (v *View) dateMatchStrings() []string {
	if v.StartDate.IsZero() || v.EndDate.IsZero() {
		return []string{"tp_year=*/tp_month=*/tp_day=*"}
	}

	var matches []string
	startYear, endYear := v.StartDate.Year(), v.EndDate.Year()
	startMonth, endMonth := v.StartDate.Month(), v.EndDate.Month()
	startDay, endDay := v.StartDate.Day(), v.EndDate.Day()

	// If the range spans multiple years, use a wildcard for the year.
	if startYear < endYear {
		return []string{fmt.Sprintf("tp_year=*/tp_month=%02d/tp_day=%02d", startMonth, startDay), fmt.Sprintf("tp_year=*/tp_month=%02d/tp_day=%02d", endMonth, endDay)}
	}

	// If the range is within the same year but spans multiple months, check for month wildcards.
	if startMonth < endMonth {
		for month := startMonth; month <= endMonth; month++ {
			if month == startMonth || month == endMonth {
				matches = append(matches, fmt.Sprintf("tp_year=%d/tp_month=%02d/tp_day=*", startYear, month))
			} else {
				matches = append(matches, fmt.Sprintf("tp_year=%d/tp_month=%02d/tp_day=*", startYear, month))
			}
		}
		return matches
	}

	// If the range is within the same month, check day-by-day.
	if startMonth == endMonth {
		for day := startDay; day <= endDay; day++ {
			matches = append(matches, fmt.Sprintf("tp_year=%d/tp_month=%02d/tp_day=%02d", startYear, startMonth, day))
		}
		return matches
	}

	// Fallback for any other case.
	return matches
}
