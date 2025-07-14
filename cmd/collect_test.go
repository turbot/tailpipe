package cmd

import (
	"reflect"
	"testing"

	"github.com/turbot/tailpipe/internal/config"
)

func Test_getPartition(t *testing.T) {
	type args struct {
		partitions []string
		name       string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "Invalid partition name",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2"},
				name:       "*",
			},
			wantErr: true,
		},
		{
			name: "Full partition name, exists",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2"},
				name:       "aws_s3_cloudtrail_log.p1",
			},
			want: []string{"aws_s3_cloudtrail_log.p1"},
		},
		{
			name: "Full partition name, does not exist",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2"},
				name:       "aws_s3_cloudtrail_log.p3",
			},
			want: nil,
		},
		{
			name: "Table name",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2"},
				name:       "aws_s3_cloudtrail_log",
			},
			want: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2"},
		},
		{
			name: "Table name (exists) with wildcard",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2"},
				name:       "aws_s3_cloudtrail_log.*",
			},
			want: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2"},
		},
		{
			name: "Table name (exists) with ?",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2"},
				name:       "aws_s3_cloudtrail_log.p?",
			},
			want: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2"},
		},
		{
			name: "Table name (exists) with non matching partition wildacard",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2"},
				name:       "aws_s3_cloudtrail_log.d*?",
			},
			want: nil,
		},
		{
			name: "Table name (does not exist)) with wildcard",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2"},
				name:       "foo.*",
			},
			want: nil,
		},
		{
			name: "Partition short name, exists",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2", "aws_elb_access_log.p1", "aws_elb_access_log.p2"},
				name:       "p1",
			},
			want: []string{"aws_s3_cloudtrail_log.p1", "aws_elb_access_log.p1"},
		},
		{
			name: "Table wildcard, partition short name, exists",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2", "aws_elb_access_log.p1", "aws_elb_access_log.p2"},
				name:       "*.p1",
			},
			want: []string{"aws_s3_cloudtrail_log.p1", "aws_elb_access_log.p1"},
		},
		{
			name: "Partition short name, does not exist",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2", "aws_elb_access_log.p1", "aws_elb_access_log.p2"},
				name:       "p3",
			},
			want: nil,
		},
		{
			name: "Table wildcard, partition short name, does not exist",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2", "aws_elb_access_log.p1", "aws_elb_access_log.p2"},
				name:       "*.p3",
			},
			want: nil,
		},
		{
			name: "Table wildcard, no dot",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2", "aws_elb_access_log.p1", "aws_elb_access_log.p2"},
				name:       "aws*",
			},
			want: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2", "aws_elb_access_log.p1", "aws_elb_access_log.p2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getPartitionsForArg(tt.args.partitions, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("getPartitions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getPartitions() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getPartitionMatchPatternsForArg(t *testing.T) {
	type args struct {
		partitions []string
		arg        string
	}
	tests := []struct {
		name             string
		args             args
		wantTablePattern string
		wantPartPattern  string
		wantErr          bool
	}{
		{
			name: "Valid table and partition pattern",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2"},
				arg:        "aws_s3_cloudtrail_log.p1",
			},
			wantTablePattern: "aws_s3_cloudtrail_log",
			wantPartPattern:  "p1",
		},
		{
			name: "Wildcard partition pattern",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2", "aws_elb_access_log.p1"},
				arg:        "aws_s3_cloudtrail_log.*",
			},
			wantTablePattern: "aws_s3_cloudtrail_log",
			wantPartPattern:  "*",
		},
		{
			name: "Wildcard in table and partition both",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2", "aws_elb_access_log.p1"},
				arg:        "aws*.*",
			},
			wantTablePattern: "aws*",
			wantPartPattern:  "*",
		},
		{
			name: "Wildcard table pattern",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_elb_access_log.p1"},
				arg:        "*.p1",
			},
			wantTablePattern: "*",
			wantPartPattern:  "p1",
		},
		{
			name: "Invalid partition name",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2"},
				arg:        "*",
			},
			wantErr: true,
		},
		{
			name: "Table exists without partition",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_s3_cloudtrail_log.p2"},
				arg:        "aws_s3_cloudtrail_log",
			},
			wantTablePattern: "aws_s3_cloudtrail_log",
			wantPartPattern:  "*",
		},
		{
			name: "Partition only, multiple tables",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1", "aws_elb_access_log.p1"},
				arg:        "p1",
			},
			wantTablePattern: "*",
			wantPartPattern:  "p1",
		},
		{
			name: "Invalid argument with multiple dots",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1"},
				arg:        "aws.s3.cloudtrail",
			},
			wantErr: true,
		},
		{
			name: "Non-existing table name",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1"},
				arg:        "non_existing_table.p1",
			},
			wantTablePattern: "non_existing_table",
			wantPartPattern:  "p1",
		},
		{
			name: "Partition name does not exist",
			args: args{
				partitions: []string{"aws_s3_cloudtrail_log.p1"},
				arg:        "p2",
			},
			wantTablePattern: "*",
			wantPartPattern:  "p2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTablePattern, gotPartPattern, err := getPartitionMatchPatternsForArg(tt.args.partitions, tt.args.arg)
			if (err != nil) != tt.wantErr {
				t.Errorf("getPartitionMatchPatternsForArg() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotTablePattern != tt.wantTablePattern {
				t.Errorf("getPartitionMatchPatternsForArg() gotTablePattern = %v, want %v", gotTablePattern, tt.wantTablePattern)
			}
			if gotPartPattern != tt.wantPartPattern {
				t.Errorf("getPartitionMatchPatternsForArg() gotPartPattern = %v, want %v", gotPartPattern, tt.wantPartPattern)
			}
		})
	}
}

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
