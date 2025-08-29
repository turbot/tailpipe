package collector

import (
	"testing"
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
		{"Only InitialFiles: Under", 10, 0, 0, defaultMax, 10, 0, 0},
		{"Only InitialFiles: Over", 20, 0, 0, defaultMax, 15, 0, 0},
		{"Only Row: Under", 0, 0, 10, defaultMax, 0, 0, 10},
		{"Only Row: Over", 0, 0, 20, defaultMax, 0, 0, 15},
		{"Adjusted Max: Odd", 10, 10, 10, 9, 3, 3, 3},
		{"Adjusted Max: Even", 10, 10, 10, 8, 4, 2, 2},
		{"Max > Available (Exhausted)", 2, 2, 1, defaultMax, 2, 2, 1},
		{"One Over Others Zero", 20, 0, 0, defaultMax, 15, 0, 0},
		{"Uneven: Cascading", 5, 10, 15, defaultMax, 5, 5, 5},
		{"Uneven: Spare To InitialFiles", 20, 3, 3, defaultMax, 9, 3, 3},
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
