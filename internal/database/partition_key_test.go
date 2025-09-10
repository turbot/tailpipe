package database

import (
	"testing"
	"time"
)

// timeString is a helper function to create time.Time from string
func timeString(timeStr string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05", timeStr)
	if err != nil {
		panic(err)
	}
	return t
}

func TestPartitionKeyRangeOperations(t *testing.T) {
	pk := &partitionKey{}

	tests := []struct {
		name     string
		testType string // "rangesOverlap", "findOverlappingFileRanges", "newUnorderedDataTimeRange"
		input    interface{}
		expected interface{}
	}{
		// Test cases for rangesOverlap function
		{
			name:     "rangesOverlap - overlapping ranges",
			testType: "rangesOverlap",
			input: struct {
				r1 fileTimeRange
				r2 fileTimeRange
			}{
				r1: fileTimeRange{min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00")},
				r2: fileTimeRange{min: timeString("2024-01-01 12:00:00"), max: timeString("2024-01-03 00:00:00")},
			},
			expected: true,
		},
		{
			name:     "rangesOverlap - non-overlapping ranges",
			testType: "rangesOverlap",
			input: struct {
				r1 fileTimeRange
				r2 fileTimeRange
			}{
				r1: fileTimeRange{min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00")},
				r2: fileTimeRange{min: timeString("2024-01-03 00:00:00"), max: timeString("2024-01-04 00:00:00")},
			},
			expected: false,
		},
		{
			name:     "rangesOverlap - touching ranges (contiguous, not overlapping)",
			testType: "rangesOverlap",
			input: struct {
				r1 fileTimeRange
				r2 fileTimeRange
			}{
				r1: fileTimeRange{min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00")},
				r2: fileTimeRange{min: timeString("2024-01-02 00:00:00"), max: timeString("2024-01-03 00:00:00")},
			},
			expected: false,
		},
		{
			name:     "rangesOverlap - identical ranges",
			testType: "rangesOverlap",
			input: struct {
				r1 fileTimeRange
				r2 fileTimeRange
			}{
				r1: fileTimeRange{min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00")},
				r2: fileTimeRange{min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00")},
			},
			expected: true,
		},
		{
			name:     "rangesOverlap - partial overlap",
			testType: "rangesOverlap",
			input: struct {
				r1 fileTimeRange
				r2 fileTimeRange
			}{
				r1: fileTimeRange{min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 12:00:00")},
				r2: fileTimeRange{min: timeString("2024-01-02 00:00:00"), max: timeString("2024-01-03 00:00:00")},
			},
			expected: true,
		},
		{
			name:     "rangesOverlap - one range completely inside another",
			testType: "rangesOverlap",
			input: struct {
				r1 fileTimeRange
				r2 fileTimeRange
			}{
				r1: fileTimeRange{min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-05 00:00:00")},
				r2: fileTimeRange{min: timeString("2024-01-02 00:00:00"), max: timeString("2024-01-03 00:00:00")},
			},
			expected: true,
		},
		{
			name:     "rangesOverlap - ranges with same start time",
			testType: "rangesOverlap",
			input: struct {
				r1 fileTimeRange
				r2 fileTimeRange
			}{
				r1: fileTimeRange{min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00")},
				r2: fileTimeRange{min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-03 00:00:00")},
			},
			expected: true,
		},
		{
			name:     "rangesOverlap - ranges with same end time",
			testType: "rangesOverlap",
			input: struct {
				r1 fileTimeRange
				r2 fileTimeRange
			}{
				r1: fileTimeRange{min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00")},
				r2: fileTimeRange{min: timeString("2024-01-01 12:00:00"), max: timeString("2024-01-02 00:00:00")},
			},
			expected: true,
		},

		// Test cases for findOverlappingFileRanges function
		{
			name:     "findOverlappingFileRanges - no overlaps",
			testType: "findOverlappingFileRanges",
			input: []fileTimeRange{
				{path: "file1", min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00"), rowCount: 1000},
				{path: "file2", min: timeString("2024-01-03 00:00:00"), max: timeString("2024-01-04 00:00:00"), rowCount: 2000},
				{path: "file3", min: timeString("2024-01-05 00:00:00"), max: timeString("2024-01-06 00:00:00"), rowCount: 1500},
			},
			expected: []unorderedDataTimeRange{},
		},
		{
			name:     "findOverlappingFileRanges - simple overlap",
			testType: "findOverlappingFileRanges",
			input: []fileTimeRange{
				{path: "file1", min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00"), rowCount: 1000},
				{path: "file2", min: timeString("2024-01-01 12:00:00"), max: timeString("2024-01-03 00:00:00"), rowCount: 2000},
			},
			expected: []unorderedDataTimeRange{
				{
					StartTime: timeString("2024-01-01 00:00:00"),
					EndTime:   timeString("2024-01-03 00:00:00"),
					RowCount:  3000,
				},
			},
		},
		{
			name:     "findOverlappingFileRanges - cross-overlapping sets",
			testType: "findOverlappingFileRanges",
			input: []fileTimeRange{
				{path: "file1", min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00"), rowCount: 1000},
				{path: "file2", min: timeString("2024-01-01 12:00:00"), max: timeString("2024-01-03 00:00:00"), rowCount: 2000},
				{path: "file3", min: timeString("2024-01-02 12:00:00"), max: timeString("2024-01-04 00:00:00"), rowCount: 1500},
				{path: "file4", min: timeString("2024-01-03 12:00:00"), max: timeString("2024-01-05 00:00:00"), rowCount: 1800},
			},
			expected: []unorderedDataTimeRange{
				{
					StartTime: timeString("2024-01-01 00:00:00"),
					EndTime:   timeString("2024-01-05 00:00:00"),
					RowCount:  6300,
				},
			},
		},
		{
			name:     "findOverlappingFileRanges - multiple separate groups",
			testType: "findOverlappingFileRanges",
			input: []fileTimeRange{
				{path: "file1", min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00"), rowCount: 1000},
				{path: "file2", min: timeString("2024-01-01 12:00:00"), max: timeString("2024-01-03 00:00:00"), rowCount: 2000},
				{path: "file3", min: timeString("2024-01-05 00:00:00"), max: timeString("2024-01-06 00:00:00"), rowCount: 1500},
				{path: "file4", min: timeString("2024-01-05 12:00:00"), max: timeString("2024-01-07 00:00:00"), rowCount: 1800},
			},
			expected: []unorderedDataTimeRange{
				{
					StartTime: timeString("2024-01-01 00:00:00"),
					EndTime:   timeString("2024-01-03 00:00:00"),
					RowCount:  3000,
				},
				{
					StartTime: timeString("2024-01-05 00:00:00"),
					EndTime:   timeString("2024-01-07 00:00:00"),
					RowCount:  3300,
				},
			},
		},
		{
			name:     "findOverlappingFileRanges - single file",
			testType: "findOverlappingFileRanges",
			input: []fileTimeRange{
				{path: "file1", min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00"), rowCount: 1000},
			},
			expected: []unorderedDataTimeRange{},
		},
		{
			name:     "findOverlappingFileRanges - empty input",
			testType: "findOverlappingFileRanges",
			input:    []fileTimeRange{},
			expected: []unorderedDataTimeRange{},
		},
		{
			name:     "findOverlappingFileRanges - three overlapping files",
			testType: "findOverlappingFileRanges",
			input: []fileTimeRange{
				{path: "file1", min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00"), rowCount: 1000},
				{path: "file2", min: timeString("2024-01-01 12:00:00"), max: timeString("2024-01-02 12:00:00"), rowCount: 2000},
				{path: "file3", min: timeString("2024-01-02 00:00:00"), max: timeString("2024-01-03 00:00:00"), rowCount: 1500},
			},
			expected: []unorderedDataTimeRange{
				{
					StartTime: timeString("2024-01-01 00:00:00"),
					EndTime:   timeString("2024-01-03 00:00:00"),
					RowCount:  4500,
				},
			},
		},
		{
			name:     "findOverlappingFileRanges - files with identical time ranges",
			testType: "findOverlappingFileRanges",
			input: []fileTimeRange{
				{path: "file1", min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00"), rowCount: 1000},
				{path: "file2", min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00"), rowCount: 2000},
			},
			expected: []unorderedDataTimeRange{
				{
					StartTime: timeString("2024-01-01 00:00:00"),
					EndTime:   timeString("2024-01-02 00:00:00"),
					RowCount:  3000,
				},
			},
		},

		// Test cases for newUnorderedDataTimeRange function
		{
			name:     "newUnorderedDataTimeRange - single file",
			testType: "newUnorderedDataTimeRange",
			input: []fileTimeRange{
				{path: "file1", min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00"), rowCount: 1000},
			},
			expected: unorderedDataTimeRange{
				StartTime: timeString("2024-01-01 00:00:00"),
				EndTime:   timeString("2024-01-02 00:00:00"),
				RowCount:  1000,
			},
		},
		{
			name:     "newUnorderedDataTimeRange - multiple overlapping files",
			testType: "newUnorderedDataTimeRange",
			input: []fileTimeRange{
				{path: "file1", min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00"), rowCount: 1000},
				{path: "file2", min: timeString("2024-01-01 12:00:00"), max: timeString("2024-01-03 00:00:00"), rowCount: 2000},
				{path: "file3", min: timeString("2024-01-02 00:00:00"), max: timeString("2024-01-04 00:00:00"), rowCount: 1500},
			},
			expected: unorderedDataTimeRange{
				StartTime: timeString("2024-01-01 00:00:00"), // earliest start
				EndTime:   timeString("2024-01-04 00:00:00"), // latest end
				RowCount:  4500,                              // sum of all row counts
			},
		},
		{
			name:     "newUnorderedDataTimeRange - files with zero row counts",
			testType: "newUnorderedDataTimeRange",
			input: []fileTimeRange{
				{path: "file1", min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00"), rowCount: 0},
				{path: "file2", min: timeString("2024-01-01 12:00:00"), max: timeString("2024-01-03 00:00:00"), rowCount: 1000},
			},
			expected: unorderedDataTimeRange{
				StartTime: timeString("2024-01-01 00:00:00"),
				EndTime:   timeString("2024-01-03 00:00:00"),
				RowCount:  1000,
			},
		},
		{
			name:     "newUnorderedDataTimeRange - files with same start time",
			testType: "newUnorderedDataTimeRange",
			input: []fileTimeRange{
				{path: "file1", min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00"), rowCount: 1000},
				{path: "file2", min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-03 00:00:00"), rowCount: 2000},
			},
			expected: unorderedDataTimeRange{
				StartTime: timeString("2024-01-01 00:00:00"),
				EndTime:   timeString("2024-01-03 00:00:00"),
				RowCount:  3000,
			},
		},
		{
			name:     "newUnorderedDataTimeRange - files with same end time",
			testType: "newUnorderedDataTimeRange",
			input: []fileTimeRange{
				{path: "file1", min: timeString("2024-01-01 00:00:00"), max: timeString("2024-01-02 00:00:00"), rowCount: 1000},
				{path: "file2", min: timeString("2024-01-01 12:00:00"), max: timeString("2024-01-02 00:00:00"), rowCount: 2000},
			},
			expected: unorderedDataTimeRange{
				StartTime: timeString("2024-01-01 00:00:00"),
				EndTime:   timeString("2024-01-02 00:00:00"),
				RowCount:  3000,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.testType {
			case "rangesOverlap":
				input := tt.input.(struct {
					r1 fileTimeRange
					r2 fileTimeRange
				})
				result := rangesOverlap(input.r1, input.r2)
				expected := tt.expected.(bool)
				if result != expected {
					t.Errorf("rangesOverlap() = %v, expected %v", result, expected)
				}

			case "findOverlappingFileRanges":
				input := tt.input.([]fileTimeRange)
				expected := tt.expected.([]unorderedDataTimeRange)
				result, err := pk.findOverlappingFileRanges(input)
				if err != nil {
					t.Fatalf("findOverlappingFileRanges() error = %v", err)
				}
				if !compareUnorderedRangesets(result, expected) {
					t.Errorf("findOverlappingFileRanges() = %v, expected %v", result, expected)
				}

			case "newUnorderedDataTimeRange":
				input := tt.input.([]fileTimeRange)
				expected := tt.expected.(unorderedDataTimeRange)
				result, err := newUnorderedDataTimeRange(input)
				if err != nil {
					t.Fatalf("newUnorderedDataTimeRange() error = %v", err)
				}
				if !result.StartTime.Equal(expected.StartTime) {
					t.Errorf("StartTime = %v, expected %v", result.StartTime, expected.StartTime)
				}
				if !result.EndTime.Equal(expected.EndTime) {
					t.Errorf("EndTime = %v, expected %v", result.EndTime, expected.EndTime)
				}
				if result.RowCount != expected.RowCount {
					t.Errorf("RowCount = %v, expected %v", result.RowCount, expected.RowCount)
				}
			}
		})
	}
}

// compareUnorderedRangesets compares two slices of unorderedDataTimeRange, ignoring order
func compareUnorderedRangesets(actual []unorderedDataTimeRange, expected []unorderedDataTimeRange) bool {
	if len(actual) != len(expected) {
		return false
	}

	// Convert to sets for comparison using time range as key
	actualSets := make(map[string]unorderedDataTimeRange)
	expectedSets := make(map[string]unorderedDataTimeRange)

	for _, set := range actual {
		key := set.StartTime.Format("2006-01-02 15:04:05") + "-" + set.EndTime.Format("2006-01-02 15:04:05")
		actualSets[key] = set
	}

	for _, set := range expected {
		key := set.StartTime.Format("2006-01-02 15:04:05") + "-" + set.EndTime.Format("2006-01-02 15:04:05")
		expectedSets[key] = set
	}

	// Check if each set in actual has a matching set in expected
	for key, actualSet := range actualSets {
		expectedSet, exists := expectedSets[key]
		if !exists || !unorderedRangesetsEqual(actualSet, expectedSet) {
			return false
		}
	}

	return true
}

// unorderedRangesetsEqual compares two unorderedDataTimeRange structs
func unorderedRangesetsEqual(a, b unorderedDataTimeRange) bool {
	return a.StartTime.Equal(b.StartTime) && a.EndTime.Equal(b.EndTime) && a.RowCount == b.RowCount
}
