package cmd

import (
	"testing"
)

func Test_getPartitionSqlFilters(t *testing.T) {
	tests := []struct {
		name        string
		partitions  []string
		args        []string
		wantFilters string
		wantErr     bool
	}{
		{
			name: "Basic partition filters with wildcard",
			partitions: []string{
				"aws_cloudtrail_log.p1",
				"aws_cloudtrail_log.p2",
				"github_audit_log.p1",
			},
			args: []string{"aws_cloudtrail_log.*", "github_audit_log.p1"},
			wantFilters: "tp_table = 'aws_cloudtrail_log' OR " +
				"(tp_table = 'github_audit_log' and tp_partition = 'p1')",
			wantErr: false,
		},
		{
			name: "Wildcard in table and exact partition",
			partitions: []string{
				"aws_cloudtrail_log.p1",
				"sys_logs.p2",
			},
			args: []string{"aws*.p1", "sys_logs.*"},
			wantFilters: "(tp_table like 'aws%' and tp_partition = 'p1') OR " +
				"tp_table = 'sys_logs'",
			wantErr: false,
		},
		{
			name: "Exact table and partition",
			partitions: []string{
				"aws_cloudtrail_log.p1",
			},
			args:        []string{"aws_cloudtrail_log.p1"},
			wantFilters: "(tp_table = 'aws_cloudtrail_log' and tp_partition = 'p1')",
			wantErr:     false,
		},
		{
			name: "Partition with full wildcard",
			partitions: []string{
				"aws_cloudtrail_log.p1",
			},
			args:        []string{"aws_cloudtrail_log.*"},
			wantFilters: "tp_table = 'aws_cloudtrail_log'",
			wantErr:     false,
		},
		{
			name: "Table with full wildcard",
			partitions: []string{
				"aws_cloudtrail_log.p1",
			},
			args:        []string{"*.p1"},
			wantFilters: "tp_partition = 'p1'",
			wantErr:     false,
		},
		{
			name: "Both table and partition with full wildcards",
			partitions: []string{
				"aws_cloudtrail_log.p1",
			},
			args:        []string{"*.*"},
			wantFilters: "",
			wantErr:     false,
		},
		{
			name:        "Empty input",
			partitions:  []string{"aws_cloudtrail_log.p1"},
			args:        []string{},
			wantFilters: "",
			wantErr:     false,
		},
		{
			name: "Multiple wildcards in table and partition",
			partitions: []string{
				"aws_cloudtrail_log.p1",
				"sys_logs.p2",
			},
			args:        []string{"aws*log.p*"},
			wantFilters: "(tp_table like 'aws%log' and tp_partition like 'p%')",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFilters, err := getPartitionSqlFilters(tt.args, tt.partitions)
			if (err != nil) != tt.wantErr {
				t.Errorf("getPartitionSqlFilters() name = %s error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}
			if gotFilters != tt.wantFilters {
				t.Errorf("getPartitionSqlFilters() name = %s got = %v, want %v", tt.name, gotFilters, tt.wantFilters)
			}
		})
	}
}

func Test_getIndexSqlFilters(t *testing.T) {
	tests := []struct {
		name        string
		indexArgs   []string
		wantFilters string
		wantErr     bool
	}{
		{
			name:      "Multiple indexes with wildcards and exact values",
			indexArgs: []string{"1234*", "456789012345", "98*76"},
			wantFilters: "cast(tp_index as varchar) like '1234%' OR " +
				"tp_index = '456789012345' OR " +
				"cast(tp_index as varchar) like '98%76'",
			wantErr: false,
		},
		{
			name:        "Single index with wildcard",
			indexArgs:   []string{"12345678*"},
			wantFilters: "cast(tp_index as varchar) like '12345678%'",
			wantErr:     false,
		},
		{
			name:        "No input provided",
			indexArgs:   []string{},
			wantFilters: "",
			wantErr:     false,
		},
		{
			name:        "Fully wildcarded index",
			indexArgs:   []string{"*"},
			wantFilters: "",
			wantErr:     false,
		},
		{
			name:        "Exact numeric index",
			indexArgs:   []string{"123456789012"},
			wantFilters: "tp_index = '123456789012'",
			wantErr:     false,
		},
		{
			name:      "Mixed patterns",
			indexArgs: []string{"12*", "3456789", "9*76"},
			wantFilters: "cast(tp_index as varchar) like '12%' OR " +
				"tp_index = '3456789' OR " +
				"cast(tp_index as varchar) like '9%76'",
			wantErr: false,
		},
		{
			name:        "Multiple exact values",
			indexArgs:   []string{"123456789012", "987654321098"},
			wantFilters: "tp_index = '123456789012' OR tp_index = '987654321098'",
			wantErr:     false,
		},
		{
			name:        "Leading and trailing spaces in exact value",
			indexArgs:   []string{" 123456789012 "},
			wantFilters: "tp_index = ' 123456789012 '", // Spaces preserved
			wantErr:     false,
		},
		{
			name:      "Combination of wildcards and exact values",
			indexArgs: []string{"*456*", "1234", "98*76"},
			wantFilters: "cast(tp_index as varchar) like '%456%' OR " +
				"tp_index = '1234' OR " +
				"cast(tp_index as varchar) like '98%76'",
			wantErr: false,
		},
		{
			name:        "Empty string as index",
			indexArgs:   []string{""},
			wantFilters: "tp_index = ''",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFilters, err := getIndexSqlFilters(tt.indexArgs)
			if (err != nil) != tt.wantErr {
				t.Errorf("getIndexSqlFilters() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotFilters != tt.wantFilters {
				t.Errorf("getIndexSqlFilters() got = %v, want %v", gotFilters, tt.wantFilters)
			}
		})
	}
}
