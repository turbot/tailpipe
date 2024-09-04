package parse

import (
	"reflect"
	"testing"
)

// result is a struct to hold the expected result of the test - designed to be easily compared with the actual result
type result struct {
	plugin          string
	partitionType   string
	partitionConfig string
	sourceType      string
	sourceConfig    string
}

func TestGetPartitionConfig(t *testing.T) {
	type args struct {
		configPath string
		partition  string
	}
	tests := []struct {
		name    string
		args    args
		want    result
		wantErr bool
	}{
		// TODO #testing add more test cases
		{
			name: "1",
			args: args{
				configPath: "test_data/configs",
				partition:  "partition.aws_cloudtrail_log.cloudtrail_logs",
			},
			want: result{
				plugin:        "aws",
				partitionType: "aws_cloudtrail_log",
				sourceType:    "file_system",
				sourceConfig: `paths = ["/Users/kai/tailpipe_data/flaws_cloudtrail_logs"]
extensions = [".gz"]`,
			},

			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := LoadTailpipeConfig()
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadTailpipeConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			col, ok := config.Partitions[tt.args.partition]
			if !ok {
				t.Errorf("LoadTailpipeConfig() partition not found")
				return
			}

			// build the result
			var got = result{
				plugin:          col.Plugin,
				partitionType:   col.Table,
				partitionConfig: string(col.Config),
				sourceType:      col.Source.Type,
				sourceConfig:    string(col.Source.Config),
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LoadTailpipeConfig() got = %v, want %v", got, tt.want)
			}
		})
	}
}
