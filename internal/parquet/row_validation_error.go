package parquet

import (
	"fmt"
)

type RowValidationError struct {
	nullColumns []string
}

func NewRowValidationError(nullColumns []string) *RowValidationError {
	return &RowValidationError{
		nullColumns: nullColumns,
	}

}

func (e *RowValidationError) Error() string {
	return fmt.Sprintf("validation failed - found null values in columns: %s", e.nullColumns)
}
