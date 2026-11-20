package cmd

import (
	"errors"
	"fmt"
	"strings"
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

func testPartition(table, name string, unconfigured bool) *config.Partition {
	p := &config.Partition{TableName: table, Unconfigured: unconfigured}
	p.ShortName = name
	p.UnqualifiedName = table + "." + name
	return p
}

func Test_partitionNotFoundError(t *testing.T) {
	unconfigured := map[string]*config.Partition{
		"aws_cloudtrail_log.removed": testPartition("aws_cloudtrail_log", "removed", true),
	}
	load := func() (map[string]*config.Partition, error) { return unconfigured, nil }

	t.Run("an unconfigured partition is said to be unconfigured", func(t *testing.T) {
		err := partitionNotFoundError("aws_cloudtrail_log.removed", load)
		if err == nil || !strings.Contains(err.Error(), "aws_cloudtrail_log.removed is not configured") {
			t.Fatalf("expected a 'not configured' error, got: %v", err)
		}
		if strings.Contains(err.Error(), "not found") {
			t.Errorf("error should not say the partition was not found: %v", err)
		}
	})
	t.Run("an unknown partition is not found", func(t *testing.T) {
		err := partitionNotFoundError("aws_cloudtrail_log.unknown", load)
		if err == nil || err.Error() != "partition not found: aws_cloudtrail_log.unknown" {
			t.Fatalf("expected a 'not found' error, got: %v", err)
		}
	})
	t.Run("a database error falls back to not found", func(t *testing.T) {
		err := partitionNotFoundError("aws_cloudtrail_log.removed", func() (map[string]*config.Partition, error) {
			return nil, errors.New("database is locked")
		})
		if err == nil || err.Error() != "partition not found: aws_cloudtrail_log.removed" {
			t.Fatalf("expected a 'not found' error, got: %v", err)
		}
	})
}

func Test_resolvePartitionArgs(t *testing.T) {
	partitions := map[string]*config.Partition{
		"aws_cloudtrail_log.kept":    testPartition("aws_cloudtrail_log", "kept", false),
		"aws_cloudtrail_log.removed": testPartition("aws_cloudtrail_log", "removed", true),
	}
	notFound := func(arg string) error { return fmt.Errorf("nf: %s", arg) }

	t.Run("an unconfigured partition can be resolved by name", func(t *testing.T) {
		got, err := resolvePartitionArgs(partitions, []string{"aws_cloudtrail_log.removed"}, notFound)
		if err != nil || len(got) != 1 || !got[0].Unconfigured {
			t.Fatalf("expected the unconfigured partition, got %v, %v", got, err)
		}
	})
	t.Run("a table arg resolves to configured and unconfigured partitions", func(t *testing.T) {
		got, err := resolvePartitionArgs(partitions, []string{"aws_cloudtrail_log"}, notFound)
		if err != nil || len(got) != 2 {
			t.Fatalf("expected both partitions, got %v, %v", got, err)
		}
	})
	t.Run("an arg matching nothing uses the not found error", func(t *testing.T) {
		_, err := resolvePartitionArgs(partitions, []string{"aws_cloudtrail_log.nope"}, notFound)
		if err == nil || err.Error() != "nf: aws_cloudtrail_log.nope" {
			t.Fatalf("expected the not found error, got %v", err)
		}
	})
}
