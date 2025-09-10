package cmd

import (
	"testing"

	"github.com/turbot/tailpipe/internal/config"
)

func Test_getSyntheticPartition(t *testing.T) {
	tests := []struct {
		name     string
		arg      string
		wantPart *config.Partition
		wantOk   bool
	}{
		{
			name:   "Valid synthetic partition",
			arg:    "synthetic_50cols_2000000rows_10000chunk_100ms",
			wantOk: true,
			wantPart: &config.Partition{
				TableName: "synthetic",
				SyntheticMetadata: &config.SyntheticMetadata{
					Columns:            50,
					Rows:               2000000,
					ChunkSize:          10000,
					DeliveryIntervalMs: 100,
				},
			},
		},
		{
			name:   "Not a synthetic partition",
			arg:    "aws_cloudtrail_log.p1",
			wantOk: false,
		},
		{
			name:   "Invalid synthetic partition format - too few parts",
			arg:    "synthetic_50cols_2000000rows_10000chunk",
			wantOk: false,
		},
		{
			name:   "Invalid synthetic partition format - too many parts",
			arg:    "synthetic_50cols_2000000rows_10000chunk_100ms_extra",
			wantOk: false,
		},
		{
			name:   "Invalid synthetic partition - non-numeric columns",
			arg:    "synthetic_abccols_2000000rows_10000chunk_100ms",
			wantOk: false,
		},
		{
			name:   "Invalid synthetic partition - non-numeric rows",
			arg:    "synthetic_50cols_abcrows_10000chunk_100ms",
			wantOk: false,
		},
		{
			name:   "Invalid synthetic partition - non-numeric chunk",
			arg:    "synthetic_50cols_2000000rows_abcchunk_100ms",
			wantOk: false,
		},
		{
			name:   "Invalid synthetic partition - non-numeric interval",
			arg:    "synthetic_50cols_2000000rows_10000chunk_abcms",
			wantOk: false,
		},
		{
			name:   "Invalid synthetic partition - zero values",
			arg:    "synthetic_0cols_2000000rows_10000chunk_100ms",
			wantOk: false,
		},
		{
			name:   "Invalid synthetic partition - negative values",
			arg:    "synthetic_-50cols_2000000rows_10000chunk_100ms",
			wantOk: false,
		},
		{
			name:   "Invalid synthetic partition - zero interval",
			arg:    "synthetic_50cols_2000000rows_10000chunk_0ms",
			wantOk: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPart, gotOk := getSyntheticPartition(tt.arg)
			if gotOk != tt.wantOk {
				t.Errorf("getSyntheticPartition() gotOk = %v, want %v", gotOk, tt.wantOk)
				return
			}
			if gotOk {
				if gotPart.TableName != tt.wantPart.TableName {
					t.Errorf("getSyntheticPartition() TableName = %v, want %v", gotPart.TableName, tt.wantPart.TableName)
				}
				if gotPart.SyntheticMetadata == nil {
					t.Errorf("getSyntheticPartition() SyntheticMetadata is nil")
					return
				}
				if gotPart.SyntheticMetadata.Columns != tt.wantPart.SyntheticMetadata.Columns {
					t.Errorf("getSyntheticPartition() Columns = %v, want %v", gotPart.SyntheticMetadata.Columns, tt.wantPart.SyntheticMetadata.Columns)
				}
				if gotPart.SyntheticMetadata.Rows != tt.wantPart.SyntheticMetadata.Rows {
					t.Errorf("getSyntheticPartition() Rows = %v, want %v", gotPart.SyntheticMetadata.Rows, tt.wantPart.SyntheticMetadata.Rows)
				}
				if gotPart.SyntheticMetadata.ChunkSize != tt.wantPart.SyntheticMetadata.ChunkSize {
					t.Errorf("getSyntheticPartition() ChunkSize = %v, want %v", gotPart.SyntheticMetadata.ChunkSize, tt.wantPart.SyntheticMetadata.ChunkSize)
				}
				if gotPart.SyntheticMetadata.DeliveryIntervalMs != tt.wantPart.SyntheticMetadata.DeliveryIntervalMs {
					t.Errorf("getSyntheticPartition() DeliveryIntervalMs = %v, want %v", gotPart.SyntheticMetadata.DeliveryIntervalMs, tt.wantPart.SyntheticMetadata.DeliveryIntervalMs)
				}
			}
		})
	}
}

func Test_getSyntheticPartition_Logging(t *testing.T) {
	// Test that logging works for various failure scenarios
	testCases := []struct {
		name string
		arg  string
	}{
		{"Invalid format", "synthetic_50cols_2000000rows_10000chunk"},
		{"Invalid columns", "synthetic_abccols_2000000rows_10000chunk_100ms"},
		{"Invalid rows", "synthetic_50cols_abcrows_10000chunk_100ms"},
		{"Invalid chunk", "synthetic_50cols_2000000rows_abcchunk_100ms"},
		{"Invalid interval", "synthetic_50cols_2000000rows_10000chunk_abcms"},
		{"Zero values", "synthetic_0cols_2000000rows_10000chunk_100ms"},
		{"Valid partition", "synthetic_50cols_2000000rows_10000chunk_100ms"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// This test ensures the function doesn't panic and handles logging gracefully
			// The actual log output would be visible when running with debug level enabled
			_, ok := getSyntheticPartition(tc.arg)

			// Just verify the function completes without error
			// The logging is a side effect that we can't easily test without capturing log output
			if tc.name == "Valid partition" && !ok {
				t.Errorf("Expected valid partition to return true")
			}
		})
	}
}
