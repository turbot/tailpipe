package parquet

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/stretchr/testify/assert"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/filepaths"
)

func Test_shouldClearInvalidState(t *testing.T) {
	tests := []struct {
		name            string
		invalidFromDate time.Time
		from            time.Time
		want            bool
	}{
		{
			name:            "both zero",
			invalidFromDate: time.Time{},
			from:            time.Time{},
			want:            true,
		},
		{
			name:            "invalidFromDate zero, from not zero",
			invalidFromDate: time.Time{},
			from:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			want:            false,
		},
		{
			name:            "from zero, invalidFromDate not zero",
			invalidFromDate: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			from:            time.Time{},
			want:            true,
		},
		{
			name:            "invalidFromDate before from",
			invalidFromDate: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			from:            time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			want:            true,
		},
		{
			name:            "invalidFromDate equal to from",
			invalidFromDate: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			from:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			want:            true,
		},
		{
			name:            "invalidFromDate after from",
			invalidFromDate: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			from:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			want:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldClearInvalidState(tt.invalidFromDate, tt.from)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_getDeleteInvalidDate(t *testing.T) {
	tests := []struct {
		name            string
		from            time.Time
		invalidFromDate time.Time
		want            time.Time
	}{
		{
			name:            "both zero",
			from:            time.Time{},
			invalidFromDate: time.Time{},
			want:            time.Time{},
		},
		{
			name:            "from zero, invalidFromDate not zero",
			from:            time.Time{},
			invalidFromDate: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			want:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:            "from not zero, invalidFromDate zero",
			from:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			invalidFromDate: time.Time{},
			want:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:            "from before invalidFromDate",
			from:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			invalidFromDate: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			want:            time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			name:            "from after invalidFromDate",
			from:            time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			invalidFromDate: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			want:            time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getDeleteInvalidDate(tt.from, tt.invalidFromDate)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_deleteInvalidParquetFiles(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "delete_invalid_parquet_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test partition
	block := &hcl.Block{
		Labels: []string{"test_table", "test_partition"},
	}
	partitionResource, _ := config.NewPartition(block, "partition.test_table.test_partition")
	partition := partitionResource.(*config.Partition)

	// Create test files with different dates
	testFiles := []struct {
		name     string
		date     time.Time
		expected bool // whether the file should be deleted
	}{
		{
			name:     "old_invalid.parquet.invalid",
			date:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: false,
		},
		{
			name:     "new_invalid.parquet.invalid",
			date:     time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
			expected: true,
		},
		{
			name:     "old_temp.parquet.tmp",
			date:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: false,
		},
		{
			name:     "new_temp.parquet.tmp",
			date:     time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
			expected: true,
		},
	}

	// Create the partition directory
	partitionDir := filepaths.GetParquetPartitionPath(tempDir, partition.TableName, partition.GetUnqualifiedName())
	if err := os.MkdirAll(partitionDir, 0755); err != nil {
		t.Fatalf("Failed to create partition dir: %v", err)
	}

	// Create test files
	for _, tf := range testFiles {
		filePath := filepath.Join(partitionDir, tf.name)
		if err := os.WriteFile(filePath, []byte("test data"), 0644); err != nil {
			t.Fatalf("Failed to create test file %s: %v", tf.name, err)
		}
	}

	// Set the from date to 2024-01-02
	from := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	// Run the delete function
	err = deleteInvalidParquetFiles(tempDir, partition, from)
	if err != nil {
		t.Fatalf("deleteInvalidParquetFiles failed: %v", err)
	}

	// Check which files were deleted
	for _, tf := range testFiles {
		filePath := filepath.Join(partitionDir, tf.name)
		_, err := os.Stat(filePath)
		exists := err == nil

		if tf.expected {
			assert.False(t, exists, "File %s should have been deleted", tf.name)
		} else {
			assert.True(t, exists, "File %s should not have been deleted", tf.name)
		}
	}
}
