package filepaths

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExtractPartitionFields(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		expected    PartitionFields
		expectError bool
	}{
		{
			name: "complete path",
			path: "/some/path/tp_table=aws_account/tp_partition=123456789/tp_date=2024-03-15/tp_index=1/file.parquet",
			expected: PartitionFields{
				Table:     "aws_account",
				Partition: "123456789",
				Date:      time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
				Index:     1,
			},
			expectError: false,
		},
		{
			name: "missing index",
			path: "/path/tp_table=aws_account/tp_partition=123456789/tp_date=2024-03-15/file.parquet",
			expected: PartitionFields{
				Table:     "aws_account",
				Partition: "123456789",
				Date:      time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
				Index:     0,
			},
			expectError: false,
		},
		{
			name: "invalid date",
			path: "/path/tp_table=aws_account/tp_partition=123456789/tp_date=invalid/tp_index=1/file.parquet",
			expected: PartitionFields{
				Table:     "aws_account",
				Partition: "123456789",
				Date:      time.Time{},
				Index:     1,
			},
			expectError: false,
		},
		{
			name: "invalid index",
			path: "/path/tp_table=aws_account/tp_partition=123456789/tp_date=2024-03-15/tp_index=invalid/file.parquet",
			expected: PartitionFields{
				Table:     "aws_account",
				Partition: "123456789",
				Date:      time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
				Index:     0,
			},
			expectError: false,
		},
		{
			name:        "empty path",
			path:        "",
			expected:    PartitionFields{},
			expectError: false,
		},
		{
			name:        "duplicate table field with different values",
			path:        "/path/tp_table=aws_account/tp_table=aws_iam/tp_partition=123456789/tp_date=2024-03-15/tp_index=1/file.parquet",
			expected:    PartitionFields{},
			expectError: true,
		},
		{
			name:        "duplicate partition field with different values",
			path:        "/path/tp_table=aws_account/tp_partition=123456789/tp_partition=987654321/tp_date=2024-03-15/tp_index=1/file.parquet",
			expected:    PartitionFields{},
			expectError: true,
		},
		{
			name:        "duplicate date field with different values",
			path:        "/path/tp_table=aws_account/tp_partition=123456789/tp_date=2024-03-15/tp_date=2024-03-16/tp_index=1/file.parquet",
			expected:    PartitionFields{},
			expectError: true,
		},
		{
			name:        "duplicate index field with different values",
			path:        "/path/tp_table=aws_account/tp_partition=123456789/tp_date=2024-03-15/tp_index=1/tp_index=2/file.parquet",
			expected:    PartitionFields{},
			expectError: true,
		},
		{
			name: "duplicate fields with same values should not error",
			path: "/path/tp_table=aws_account/tp_table=aws_account/tp_partition=123456789/tp_partition=123456789/tp_date=2024-03-15/tp_date=2024-03-15/tp_index=1/tp_index=1/file.parquet",
			expected: PartitionFields{
				Table:     "aws_account",
				Partition: "123456789",
				Date:      time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
				Index:     1,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ExtractPartitionFields(tt.path)
			if tt.expectError {
				assert.Error(t, err)
				assert.Empty(t, result)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
