package parquet

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/stretchr/testify/assert"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/filepaths"
)

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

	// Create test files
	testFiles := []struct {
		name     string
		expected bool // whether the file should be deleted
	}{
		{
			name:     "old_invalid.parquet.invalid",
			expected: true,
		},
		{
			name:     "new_invalid.parquet.invalid",
			expected: true,
		},
		{
			name:     "old_temp.parquet.tmp",
			expected: true,
		},
		{
			name:     "new_temp.parquet.tmp",
			expected: true,
		},
		{
			name:     "valid.parquet",
			expected: false,
		},
	}

	// Create the partition directory
	partitionDir := filepaths.GetParquetPartitionPath(tempDir, partition.TableName, partition.ShortName)
	if err := os.MkdirAll(partitionDir, 0755); err != nil {
		t.Fatalf("Failed to create partition dir: %v", err)
	}

	// Create test files
	for _, tf := range testFiles {
		filePath := filepath.Join(partitionDir, tf.name)
		if err := os.WriteFile(filePath, []byte("test data"), 0644); err != nil { //nolint:gosec // test code
			t.Fatalf("Failed to create test file %s: %v", tf.name, err)
		}
	}

	// Debug: Print directory structure
	err = filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(tempDir, path)
		if rel == "." {
			return nil
		}
		if info.IsDir() {
			t.Logf("DIR: %s", rel)
		} else {
			t.Logf("FILE: %s", rel)
		}
		return nil
	})
	if err != nil {
		t.Logf("Error walking directory: %v", err)
	}

	// Debug: Print glob pattern
	invalidGlob := filepaths.GetTempAndInvalidParquetFileGlobForPartition(tempDir, partition.TableName, partition.ShortName)
	t.Logf("Glob pattern: %s", invalidGlob)

	// Run the delete function
	patterns := []PartitionPattern{NewPartitionPattern(partition)}
	err = deleteInvalidParquetFiles(tempDir, patterns)
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

func Test_deleteInvalidParquetFilesWithWildcards(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "delete_invalid_parquet_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test partitions
	partitions := []struct {
		table     string
		partition string
	}{
		{"aws_cloudtrail", "cloudtrail"},
		{"aws_cloudtrail", "cloudwatch"},
		{"aws_ec2", "instances"},
		{"aws_ec2", "volumes"},
	}

	// Create test files for each partition
	testFiles := []struct {
		name     string
		expected bool
	}{
		{
			name:     "invalid.parquet.invalid",
			expected: true,
		},
		{
			name:     "temp.parquet.tmp",
			expected: true,
		},
		{
			name:     "valid.parquet",
			expected: false,
		},
	}

	// Create directories and files for each partition
	for _, p := range partitions {
		partitionDir := filepaths.GetParquetPartitionPath(tempDir, p.table, p.partition)
		if err := os.MkdirAll(partitionDir, 0755); err != nil {
			t.Fatalf("Failed to create partition dir: %v", err)
		}

		for _, tf := range testFiles {
			filePath := filepath.Join(partitionDir, tf.name)
			if err := os.WriteFile(filePath, []byte("test data"), 0644); err != nil { //nolint:gosec // test code
				t.Fatalf("Failed to create test file %s: %v", tf.name, err)
			}
		}
	}

	// Test cases with different wildcard patterns
	tests := []struct {
		name     string
		patterns []PartitionPattern
		deleted  map[string]bool // key is "table/partition", value is whether files should be deleted
	}{
		{
			name: "match all aws_cloudtrail partitions",
			patterns: []PartitionPattern{{
				Table:     "aws_cloudtrail",
				Partition: "*",
			}},
			deleted: map[string]bool{
				"aws_cloudtrail/cloudtrail": true,
				"aws_cloudtrail/cloudwatch": true,
				"aws_ec2/instances":         false,
				"aws_ec2/volumes":           false,
			},
		},
		{
			name: "match all aws_* tables",
			patterns: []PartitionPattern{{
				Table:     "aws_*",
				Partition: "*",
			}},
			deleted: map[string]bool{
				"aws_cloudtrail/cloudtrail": true,
				"aws_cloudtrail/cloudwatch": true,
				"aws_ec2/instances":         true,
				"aws_ec2/volumes":           true,
			},
		},
		{
			name: "match specific partitions across tables",
			patterns: []PartitionPattern{
				{Table: "aws_cloudtrail", Partition: "cloudtrail"},
				{Table: "aws_ec2", Partition: "instances"},
			},
			deleted: map[string]bool{
				"aws_cloudtrail/cloudtrail": true,
				"aws_cloudtrail/cloudwatch": false,
				"aws_ec2/instances":         true,
				"aws_ec2/volumes":           false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Run the delete function
			err = deleteInvalidParquetFiles(tempDir, tt.patterns)
			if err != nil {
				t.Fatalf("deleteInvalidParquetFiles failed: %v", err)
			}

			// Check each partition
			for _, p := range partitions {
				partitionDir := filepaths.GetParquetPartitionPath(tempDir, p.table, p.partition)
				key := fmt.Sprintf("%s/%s", p.table, p.partition)
				shouldDelete := tt.deleted[key]

				// Check each file
				for _, tf := range testFiles {
					filePath := filepath.Join(partitionDir, tf.name)
					_, err := os.Stat(filePath)
					exists := err == nil

					if shouldDelete && tf.expected {
						assert.False(t, exists, "[%s] File %s should have been deleted", key, tf.name)
					} else {
						assert.True(t, exists, "[%s] File %s should not have been deleted", key, tf.name)
					}
				}
			}

			// Recreate the files for the next test
			for _, p := range partitions {
				partitionDir := filepaths.GetParquetPartitionPath(tempDir, p.table, p.partition)
				for _, tf := range testFiles {
					filePath := filepath.Join(partitionDir, tf.name)
					if err := os.WriteFile(filePath, []byte("test data"), 0644); err != nil { //nolint:gosec // test code
						t.Fatalf("Failed to recreate test file %s: %v", tf.name, err)
					}
				}
			}
		})
	}
}

//func Test_shouldClearInvalidState(t *testing.T) {
//	tests := []struct {
//		name            string
//		invalidFromDate time.Time
//		from            time.Time
//		want            bool
//	}{
//		{
//			name:            "both zero",
//			invalidFromDate: time.Time{},
//			from:            time.Time{},
//			want:            true,
//		},
//		{
//			name:            "invalidFromDate zero, from not zero",
//			invalidFromDate: time.Time{},
//			from:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
//			want:            false,
//		},
//		{
//			name:            "from zero, invalidFromDate not zero",
//			invalidFromDate: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
//			from:            time.Time{},
//			want:            true,
//		},
//		{
//			name:            "invalidFromDate before from",
//			invalidFromDate: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
//			from:            time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
//			want:            true,
//		},
//		{
//			name:            "invalidFromDate equal to from",
//			invalidFromDate: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
//			from:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
//			want:            true,
//		},
//		{
//			name:            "invalidFromDate after from",
//			invalidFromDate: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
//			from:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
//			want:            false,
//		},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got := shouldClearInvalidState(tt.invalidFromDate, tt.from)
//			assert.Equal(t, tt.want, got)
//		})
//	}
//}
//
//func Test_getDeleteInvalidDate(t *testing.T) {
//	tests := []struct {
//		name            string
//		from            time.Time
//		invalidFromDate time.Time
//		want            time.Time
//	}{
//		{
//			name:            "both zero",
//			from:            time.Time{},
//			invalidFromDate: time.Time{},
//			want:            time.Time{},
//		},
//		{
//			name:            "from zero, invalidFromDate not zero",
//			from:            time.Time{},
//			invalidFromDate: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
//			want:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
//		},
//		{
//			name:            "from not zero, invalidFromDate zero",
//			from:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
//			invalidFromDate: time.Time{},
//			want:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
//		},
//		{
//			name:            "from before invalidFromDate",
//			from:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
//			invalidFromDate: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
//			want:            time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
//		},
//		{
//			name:            "from after invalidFromDate",
//			from:            time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
//			invalidFromDate: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
//			want:            time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
//		},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got := getDeleteInvalidDate(tt.from, tt.invalidFromDate)
//			assert.Equal(t, tt.want, got)
//		})
//	}
//}
