package collector

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestErrorCountsToDisplay(t *testing.T) {
	defaultMax := 15
	tests := []struct {
		name         string
		srcErrCount  int
		convErrCount int
		rowErrCount  int
		max          int
		wantSrc      int
		wantConv     int
		wantRow      int
	}{
		{"All Types: Over", 10, 10, 10, defaultMax, 5, 5, 5},
		{"All Types: Under", 2, 2, 2, defaultMax, 2, 2, 2},
		{"Only Source: Under", 10, 0, 0, defaultMax, 10, 0, 0},
		{"Only Source: Over", 20, 0, 0, defaultMax, 15, 0, 0},
		{"Only Row: Under", 0, 0, 10, defaultMax, 0, 0, 10},
		{"Only Row: Over", 0, 0, 20, defaultMax, 0, 0, 15},
		{"Adjusted Max: Odd", 10, 10, 10, 9, 3, 3, 3},
		{"Adjusted Max: Even", 10, 10, 10, 8, 4, 2, 2},
		{"Max > Available (Exhausted)", 2, 2, 1, defaultMax, 2, 2, 1},
		{"One Over Others Zero", 20, 0, 0, defaultMax, 15, 0, 0},
		{"Uneven: Cascading", 5, 10, 15, defaultMax, 5, 5, 5},
		{"Uneven: Spare To Source", 20, 3, 3, defaultMax, 9, 3, 3},
		{"Uneven: Spare To Conversion", 3, 20, 3, defaultMax, 3, 9, 3},
		{"Uneven: Spare To Row", 3, 3, 20, defaultMax, 3, 3, 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSrc, gotConv, gotRow := errorCountsToDisplay(tt.srcErrCount, tt.convErrCount, tt.rowErrCount, tt.max)
			if gotSrc != tt.wantSrc || gotConv != tt.wantConv || gotRow != tt.wantRow {
				t.Errorf("got (src:%d, conv:%d, row:%d), want (src:%d, conv:%d, row:%d)",
					gotSrc, gotConv, gotRow, tt.wantSrc, tt.wantConv, tt.wantRow)
			}
			displayed := gotSrc + gotConv + gotRow
			if displayed > tt.max {
				t.Errorf("total displayed errors %d exceeds max %d", displayed, tt.max)
			}
		})
	}
}

// a conversion error (for example a partition filter naming a missing column) shows the database's error and counts
// its rows as errors, not as saved
func TestStatusString_ConversionError(t *testing.T) {
	s := &status{partitionName: "t.p", started: time.Now()}
	s.RowsReceived = 5
	s.RowsEnriched = 5
	s.UpdateConversionStatus(0, 5, errors.New(`exec-0.jsonl: failed to read chunk: Binder Error: Referenced column "invalid_column" not found in FROM clause!`))

	out := s.String()
	if !strings.Contains(out, `Referenced column "invalid_column" not found in FROM clause!`) {
		t.Errorf("output does not show the database error:\n%s", out)
	}
	if !strings.Contains(out, "Saved:    0") {
		t.Errorf("expected 0 rows saved:\n%s", out)
	}
	if !strings.Contains(out, "Errors:   5") {
		t.Errorf("expected 5 rows reported as errors:\n%s", out)
	}
}

// the happy side: no conversion errors means no Errors lines at all
func TestStatusString_NoConversionError(t *testing.T) {
	s := &status{partitionName: "t.p", started: time.Now()}
	s.RowsReceived = 5
	s.RowsEnriched = 5
	s.UpdateConversionStatus(5, 0)

	out := s.String()
	if strings.Contains(out, "Errors:") {
		t.Errorf("expected no errors section:\n%s", out)
	}
	if !strings.Contains(out, "Saved:    5") {
		t.Errorf("expected 5 rows saved:\n%s", out)
	}
}
