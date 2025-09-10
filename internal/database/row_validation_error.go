package database

import (
	"fmt"
	"strings"

	"github.com/turbot/pipe-fittings/v2/utils"
)

type RowValidationError struct {
	nullColumns []string
	failedRows  int64
}

func NewRowValidationError(failedRows int64, nullColumns []string) *RowValidationError {
	return &RowValidationError{
		nullColumns: nullColumns,
		failedRows:  failedRows,
	}
}

func (e *RowValidationError) Error() string {
	return fmt.Sprintf("%d %s failed validation - found null values in %d %s: %s", e.failedRows, utils.Pluralize("row", int(e.failedRows)), len(e.nullColumns), utils.Pluralize("column", len(e.nullColumns)), strings.Join(e.nullColumns, ", "))
}

// Is implements the errors.Is interface to support error comparison
func (e *RowValidationError) Is(target error) bool {
	_, ok := target.(*RowValidationError)
	return ok
}
