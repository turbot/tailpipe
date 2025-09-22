package database

import (
	"fmt"
	"strings"
)

type ColumnSchemaChange struct {
	Name    string
	OldType string
	NewType string
}

type SchemaChangeError struct {
	ChangedColumns []ColumnSchemaChange
}

func (e *SchemaChangeError) Error() string {
	changeStrings := make([]string, len(e.ChangedColumns))
	for i, change := range e.ChangedColumns {
		changeStrings[i] = fmt.Sprintf("'%s': '%s' -> '%s'", change.Name, change.OldType, change.NewType)
	}
	return fmt.Sprintf("inferred schema change detected - consider specifying a column type in table definition: %s", strings.Join(changeStrings, ", "))
}
