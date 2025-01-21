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
			name: "Basic partition filters",
			partitions: []string{
				"aws_cloudtrail_log.p1",
				"aws_cloudtrail_log.p2",
				"github_audit_log.p1",
			},
			args: []string{"aws_cloudtrail_log.*", "github_audit_log.p1"},
			wantFilters: "(tp_table LIKE 'aws_cloudtrail_log' AND CAST(tp_partition AS VARCHAR) LIKE '%') OR " +
				"(tp_table LIKE 'github_audit_log' AND CAST(tp_partition AS VARCHAR) LIKE 'p1')",
			wantErr: false,
		},
		{
			name: "Wildcard in table and partition",
			partitions: []string{
				"aws_cloudtrail_log.p1",
				"sys_logs.p2",
			},
			args: []string{"aws*.p1", "sys_logs.*"},
			wantFilters: "(tp_table LIKE 'aws%' AND CAST(tp_partition AS VARCHAR) LIKE 'p1') OR " +
				"(tp_table LIKE 'sys_logs' AND CAST(tp_partition AS VARCHAR) LIKE '%')",
			wantErr: false,
		},
		{
			name:        "Empty input",
			partitions:  []string{"aws_cloudtrail_log.p1"},
			args:        []string{},
			wantFilters: "",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFilters, err := getPartitionSqlFilters(tt.args, tt.partitions)
			if (err != nil) != tt.wantErr {
				t.Errorf("getSQLFilters() name = %s error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}
			if gotFilters != tt.wantFilters {
				t.Errorf("getSQLFilters() name = %s got = %v, want %v", tt.name, gotFilters, tt.wantFilters)
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
			wantFilters: "CAST(tp_index AS VARCHAR) LIKE '1234%' OR " +
				"tp_index = '456789012345' OR " +
				"CAST(tp_index AS VARCHAR) LIKE '98%76'",
			wantErr: false,
		},
		{
			name:        "Single index with wildcard",
			indexArgs:   []string{"12345678*"},
			wantFilters: "CAST(tp_index AS VARCHAR) LIKE '12345678%'",
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
			wantFilters: "CAST(tp_index AS VARCHAR) LIKE '%'",
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
			wantFilters: "CAST(tp_index AS VARCHAR) LIKE '12%' OR " +
				"tp_index = '3456789' OR " +
				"CAST(tp_index AS VARCHAR) LIKE '9%76'",
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
			wantFilters: "CAST(tp_index AS VARCHAR) LIKE '%456%' OR " +
				"tp_index = '1234' OR " +
				"CAST(tp_index AS VARCHAR) LIKE '98%76'",
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
