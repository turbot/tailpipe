package parquet

import (
	"errors"
	"testing"
)

func TestConversionError_WrapsRowValidationError(t *testing.T) {
	// Arrange
	rve := NewRowValidationError(5, []string{"colA", "colB"})
	convErr := NewConversionError(rve, 5, "/path/to/file.jsonl")

	// Act: check with errors.Is
	match := errors.Is(convErr, &RowValidationError{})

	// Assert
	if !match {
		t.Fatalf("expected errors.Is to match *RowValidationError, but got false")
	}

	// Also: errors.As to extract
	var out *RowValidationError
	if !errors.As(convErr, &out) {
		t.Fatalf("expected errors.As to extract *RowValidationError, but failed")
	}

	if out.failedRows != 5 {
		t.Errorf("unexpected failedRows: got %d, want 5", out.failedRows)
	}
}
