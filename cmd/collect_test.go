package cmd

import (
	"reflect"
	"testing"
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
