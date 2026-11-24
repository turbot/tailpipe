package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/turbot/pipe-fittings/v2/versionfile"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/error_helpers"
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
		err := partitionNotFoundError("aws_cloudtrail_log.removed", testConfig(nil), load)
		if err == nil || !strings.Contains(err.Error(), "aws_cloudtrail_log.removed is not configured") {
			t.Fatalf("expected a 'not configured' error, got: %v", err)
		}
		if strings.Contains(err.Error(), "not found") {
			t.Errorf("error should not say the partition was not found: %v", err)
		}
	})
	t.Run("an unknown partition is not found", func(t *testing.T) {
		err := partitionNotFoundError("aws_cloudtrail_log.unknown", testConfig(nil), load)
		if err == nil || err.Error() != "partition not found: aws_cloudtrail_log.unknown" {
			t.Fatalf("expected a 'not found' error, got: %v", err)
		}
	})
	t.Run("a database error falls back to not found", func(t *testing.T) {
		err := partitionNotFoundError("aws_cloudtrail_log.removed", testConfig(nil), func() (map[string]*config.Partition, error) {
			return nil, errors.New("database is locked")
		})
		if err == nil || err.Error() != "partition not found: aws_cloudtrail_log.removed" {
			t.Fatalf("expected a 'not found' error, got: %v", err)
		}
	})
}

func testConfig(partitions map[string]*config.Partition) *config.TailpipeConfig {
	return &config.TailpipeConfig{
		Partitions:   partitions,
		CustomTables: map[string]*config.Table{"my_custom_table": {}},
		PluginVersions: map[string]*versionfile.InstalledVersion{
			"hub.tailpipe.io/plugins/turbot/github@latest": {Metadata: map[string][]string{"tables": {"github_audit_log"}}},
		},
	}
}

func Test_partitionNotFoundError_tableWithNoPartitions(t *testing.T) {
	none := func() (map[string]*config.Partition, error) { return nil, nil }
	const noPartitions = "no partitions are configured for table github_audit_log"

	t.Run("a table with no partitions says so", func(t *testing.T) {
		err := partitionNotFoundError("github_audit_log", testConfig(nil), none)
		if err == nil || !strings.Contains(err.Error(), noPartitions) || !strings.Contains(err.Error(), "partition \"github_audit_log\"") {
			t.Fatalf("expected a 'no partitions are configured' error with how to add one, got: %v", err)
		}
		if strings.Contains(err.Error(), "not found") {
			t.Errorf("error should not say not found: %v", err)
		}
	})
	t.Run("a partition of a table with no partitions says so", func(t *testing.T) {
		err := partitionNotFoundError("github_audit_log.mine", testConfig(nil), none)
		if err == nil || !strings.Contains(err.Error(), noPartitions) {
			t.Fatalf("expected a 'no partitions are configured' error, got: %v", err)
		}
	})
	t.Run("the table check is not case sensitive and names the table as it is spelt", func(t *testing.T) {
		err := partitionNotFoundError("GitHub_Audit_Log", testConfig(nil), none)
		if err == nil || !strings.Contains(err.Error(), "no partitions are configured for table github_audit_log") || strings.Contains(err.Error(), "GitHub_Audit_Log") {
			t.Fatalf("expected a 'no partitions are configured' error, got: %v", err)
		}
	})
	t.Run("a custom table with no partitions says so", func(t *testing.T) {
		err := partitionNotFoundError("my_custom_table", testConfig(nil), none)
		if err == nil || !strings.Contains(err.Error(), "no partitions are configured for table my_custom_table") {
			t.Fatalf("expected a 'no partitions are configured' error, got: %v", err)
		}
	})
	t.Run("a table with no partitions says so when the database cannot be read", func(t *testing.T) {
		err := partitionNotFoundError("github_audit_log", testConfig(nil), func() (map[string]*config.Partition, error) {
			return nil, errors.New("database is locked")
		})
		if err == nil || !strings.Contains(err.Error(), noPartitions) {
			t.Fatalf("expected a 'no partitions are configured' error, got: %v", err)
		}
	})
	t.Run("an unknown table is not found", func(t *testing.T) {
		err := partitionNotFoundError("no_such_table", testConfig(nil), none)
		if err == nil || err.Error() != "partition not found: no_such_table" {
			t.Fatalf("expected a 'not found' error, got: %v", err)
		}
	})
	t.Run("an unknown partition of an unknown table is not found", func(t *testing.T) {
		err := partitionNotFoundError("no_such_table.mine", testConfig(nil), none)
		if err == nil || err.Error() != "partition not found: no_such_table.mine" {
			t.Fatalf("expected a 'not found' error, got: %v", err)
		}
	})
	t.Run("an unknown partition of a table which has partitions is not found", func(t *testing.T) {
		cfg := testConfig(map[string]*config.Partition{
			"github_audit_log.kept": testPartition("github_audit_log", "kept", false),
		})
		err := partitionNotFoundError("github_audit_log.other", cfg, none)
		if err == nil || err.Error() != "partition not found: github_audit_log.other" {
			t.Fatalf("expected a 'not found' error, got: %v", err)
		}
	})
	t.Run("a pattern is not taken for a table", func(t *testing.T) {
		err := partitionNotFoundError("github_*.mine", testConfig(nil), none)
		if err == nil || err.Error() != "partition not found: github_*.mine" {
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

func TestCollectPartitionsSerially(t *testing.T) {
	parts := []*config.Partition{{TableName: "a"}, {TableName: "b"}, {TableName: "c"}}

	collectedWith := func(ctx context.Context, fn func(p *config.Partition) error) ([]string, error) {
		var collected []string
		err := collectPartitionsSerially(ctx, parts, func(p *config.Partition) error {
			collected = append(collected, p.TableName)
			return fn(p)
		})
		return collected, err
	}

	t.Run("collects every partition", func(t *testing.T) {
		collected, err := collectedWith(context.Background(), func(*config.Partition) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(collected, ","); got != "a,b,c" {
			t.Fatalf("collected %s, want a,b,c", got)
		}
	})

	t.Run("an error does not stop the other partitions, and all are reported", func(t *testing.T) {
		collected, err := collectedWith(context.Background(), func(p *config.Partition) error {
			if p.TableName != "b" {
				return nil
			}
			return errors.New("b failed")
		})
		if len(collected) != 3 {
			t.Fatalf("collected %v, want all three", collected)
		}
		if err == nil || !strings.Contains(err.Error(), "collection error: b failed") {
			t.Fatalf("err = %v, want it to report b's error", err)
		}
	})

	t.Run("a cancel stops the remaining partitions starting, and is reported as a cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		collected, err := collectedWith(ctx, func(p *config.Partition) error {
			cancel() // Ctrl+C during the first partition, which then ends without an error
			return nil
		})
		if got := strings.Join(collected, ","); got != "a" {
			t.Fatalf("collected %s after the cancel, want only a", got)
		}
		if !errors.Is(err, context.Canceled) || !error_helpers.IsCancelledError(err) {
			t.Fatalf("err = %v, want a cancelled error", err)
		}
	})

	t.Run("a cancel during the last partition changes nothing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := collectedWith(ctx, func(p *config.Partition) error {
			if p.TableName == "c" {
				cancel()
			}
			return nil
		})
		if err != nil {
			t.Fatalf("err = %v, want none: every partition was collected", err)
		}
	})
}
