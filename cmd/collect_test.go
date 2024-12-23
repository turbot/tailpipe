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
			name: "Invvalid partition name",
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getPartition(tt.args.partitions, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("getPartition() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getPartition() got = %v, want %v", got, tt.want)
			}
		})
	}
}
