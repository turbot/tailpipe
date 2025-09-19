package migration

import "fmt"

// UnsupportedError represents an error when migration is not supported
// due to specific command line arguments or configuration
type UnsupportedError struct {
	Reason string
}

func (e *UnsupportedError) Error() string {
	msgFormat := "data must be migrated to Ducklake format - migration is not supported with '%s'.\n\nRun 'tailpipe query' to migrate your data to DuckLake format"
	return fmt.Sprintf(msgFormat, e.Reason)
}

func (e *UnsupportedError) Is(target error) bool {
	_, ok := target.(*UnsupportedError)
	return ok
}

func (e *UnsupportedError) As(target interface{}) bool {
	if t, ok := target.(**UnsupportedError); ok {
		*t = e
		return true
	}
	return false
}
