package migration

import (
	"fmt"
)

// MigrationError is an aggregate error that wraps multiple child errors
// encountered during migration.
type MigrationError struct {
	errors []error
}

func NewMigrationError() *MigrationError {
	return &MigrationError{errors: make([]error, 0)}
}

func (m *MigrationError) Append(err error) {
	if err == nil {
		return
	}
	m.errors = append(m.errors, err)
}

func (m *MigrationError) Len() int { return len(m.errors) }

// Error provides a compact summary string
func (m *MigrationError) Error() string {
	return fmt.Sprintf("%d error(s) occurred during migration", len(m.errors))
}

// Unwrap returns the list of child errors so that errors.Is/As can walk them
// (supported since Go 1.20 with Unwrap() []error)
func (m *MigrationError) Unwrap() []error { return m.errors }
