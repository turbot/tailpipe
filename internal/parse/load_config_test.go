package parse

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/v2/app_specific"
	"github.com/turbot/pipe-fittings/v2/hclhelpers"
	"github.com/turbot/pipe-fittings/v2/modconfig"
	"github.com/turbot/pipe-fittings/v2/plugin"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/pipe-fittings/v2/versionfile"
	"github.com/turbot/tailpipe/internal/config"
)

// TODO enable and fix this test https://github.com/turbot/tailpipe/issues/506
func TestLoadTailpipeConfig(t *testing.T) {
	type args struct {
		configPath string
		partition  string
	}
	tests := []struct {
		name    string
		args    args
		want    *config.TailpipeConfig
		wantErr bool
	}{
		// TODO #testing add more test cases https://github.com/turbot/tailpipe/issues/506
		{
			name: "static tables",
			args: args{
				configPath: "test_data/static_table_config",
				// partition:  "partition.aws_cloudtrail_log.cloudtrail_logs",
			},
			want: &config.TailpipeConfig{
				PluginVersions: map[string]*versionfile.InstalledVersion{},
				Partitions: map[string]*config.Partition{
					"aws_cloudtrail_log.cloudtrail_logs": {
						TableName: "aws_cloudtrail_log",
						Source: config.Source{
							Type: "file_system",
							Config: &config.HclBytes{
								Hcl: []byte("extensions = [\".csv\"]\npaths = [\"/Users/kai/tailpipe_data/logs\"]"),
								Range: hclhelpers.NewRange(hcl.Range{
									Filename: "test_data/static_table_config/resources.tpc",
									Start: hcl.Pos{
										Line:   6,
										Column: 6,
										Byte:   157,
									},
									End: hcl.Pos{
										Line:   7,
										Column: 29,
										Byte:   244,
									},
								}),
							},
						},
						Config: []byte("    plugin = \"aws\"\n"),
						ConfigRange: hclhelpers.NewRange(hcl.Range{
							Filename: "test_data/static_table_config/resources.tpc",
							Start: hcl.Pos{
								Line:   4,
								Column: 5,
								Byte:   109,
							},
							End: hcl.Pos{
								Line:   4,
								Column: 19,
								Byte:   123,
							},
						}),
					},
					"aws_vpc_flow_log.flow_logs": {
						TableName: "aws_vpc_flow_log",
						Source: config.Source{
							Type: "aws_cloudwatch",
							Config: &config.HclBytes{
								Hcl: []byte(
									"log_group_name = \"/victor/vpc/flowlog\"\n" +
										"start_time = \"2024-08-12T07:56:26Z\"\n" +
										"end_time = \"2024-08-13T07:56:26Z\"\n" +
										"access_key = \"REPLACE\"\n" +
										"secret_key = \"REPLACE\"\n" +
										"session_token = \"REPLACE\"",
								),
								Range: hclhelpers.NewRange(hcl.Range{
									Filename: "test_data/static_table_config/resources.tpc",
									Start:    hcl.Pos{Line: 15, Column: 6, Byte: 408},
									End:      hcl.Pos{Line: 21, Column: 28, Byte: 628},
								}),
							},
						},
						// Unknown attr captured at partition level
						Config: []byte("    plugin = \"aws\"\n"),
						ConfigRange: hclhelpers.NewRange(hcl.Range{
							Filename: "test_data/static_table_config/resources.tpc",
							Start:    hcl.Pos{Line: 13, Column: 5, Byte: 357},
							End:      hcl.Pos{Line: 13, Column: 19, Byte: 371},
						}),
					},
				},
				Connections:  map[string]*config.TailpipeConnection{},
				CustomTables: map[string]*config.Table{},
				Formats:      map[string]*config.Format{},
			},

			wantErr: false,
		},
		{
			name: "dynamic tables",
			args: args{
				configPath: "test_data/custom_table_config",
				// partition:  "partition.aws_cloudtrail_log.cloudtrail_logs",
			},
			want: &config.TailpipeConfig{
				Partitions: map[string]*config.Partition{
					"my_csv_log.test": {
						HclResourceImpl: modconfig.HclResourceImpl{
							FullName:        "my_csv_log.test",
							ShortName:       "test",
							UnqualifiedName: "my_csv_log.test",
							DeclRange: hcl.Range{
								Filename: "test_data/custom_table_config/resources.tpc",
								Start: hcl.Pos{
									Line:   2,
									Column: 30,
									Byte:   30,
								},
								End: hcl.Pos{
									Line:   10,
									Column: 2,
									Byte:   230,
								},
							},
							BlockType: "partition",
						},
						TableName: "my_csv_log",
						Plugin: &plugin.Plugin{
							Instance: "custom",
							Alias:    "custom",
							Plugin:   "/plugins/turbot/custom@latest",
						},
						Source: config.Source{
							Type: "file_system",
							Config: &config.HclBytes{
								Hcl: []byte("extensions = [\".csv\"]\npaths = [\"/Users/kai/tailpipe_data/logs\"]"),
								Range: hclhelpers.NewRange(hcl.Range{
									Filename: "test_data/custom_table_config/resources.tpc",
									Start: hcl.Pos{
										Line:   4,
										Column: 9,
										Byte:   68,
									},
									End: hcl.Pos{
										Line:   5,
										Column: 30,
										Byte:   139,
									},
								}),
							},
						},
					},
				},
				CustomTables: map[string]*config.Table{
					"my_csv_log": {
						HclResourceImpl: modconfig.HclResourceImpl{
							FullName:        "table.my_csv_log",
							ShortName:       "my_csv_log",
							UnqualifiedName: "my_csv_log",
							DeclRange: hcl.Range{
								Filename: "test_data/custom_table_config/resources.tpc",
								Start: hcl.Pos{
									Line:   14,
									Column: 21,
									Byte:   295,
								},
								End: hcl.Pos{
									Line:   29,
									Column: 2,
									Byte:   602,
								},
							},
							BlockType: "table",
						},
						//Mode: schema.ModePartial,
						Columns: []config.Column{
							{
								Name:   "tp_timestamp",
								Source: utils.ToPointer("time_local"),
							},
							{
								Name:   "tp_index",
								Source: utils.ToPointer("account_id"),
							},
							{
								Name:   "org_id",
								Source: utils.ToPointer("org"),
							},
							{
								Name: "user_id",
								Type: utils.ToPointer("varchar"),
							},
						},
					},
				},
				Connections: map[string]*config.TailpipeConnection{},
				Formats: map[string]*config.Format{
					"delimited.csv_default_logs": {
						Type: "delimited",
						HclResourceImpl: modconfig.HclResourceImpl{
							FullName:        "delimited.csv_default_logs",
							ShortName:       "csv_default_logs",
							UnqualifiedName: "delimited.csv_default_logs",
							DeclRange: hcl.Range{
								Filename: "test_data/custom_table_config/resources.tpc",
								Start: hcl.Pos{
									Line:   33,
									Column: 39,
									Byte:   644,
								},
								End: hcl.Pos{
									Line:   35,
									Column: 2,
									Byte:   648,
								},
							},
							BlockType: "format",
						},
					},
					"delimited.csv_logs": {
						Type: "delimited",
						HclResourceImpl: modconfig.HclResourceImpl{
							FullName:        "delimited.csv_logs",
							ShortName:       "csv_logs",
							UnqualifiedName: "delimited.csv_logs",
							DeclRange: hcl.Range{
								Filename: "test_data/custom_table_config/resources.tpc",
								Start: hcl.Pos{
									Line:   37,
									Column: 32,
									Byte:   681,
								},
								End: hcl.Pos{
									Line:   40,
									Column: 2,
									Byte:   743,
								},
							},
							BlockType: "format",
						},
						Config: &config.HclBytes{
							Hcl: []byte(
								"    header            = false\n\n    delimiter         = \"\\t\"\n",
							),
							Range: hclhelpers.NewRange(hcl.Range{
								Filename: "test_data/static_table_config/resources.tpc",
								Start:    hcl.Pos{Line: 38, Column: 5, Byte: 687},
								End:      hcl.Pos{Line: 39, Column: 30, Byte: 741},
							}),
						},
					},
				},
				PluginVersions: map[string]*versionfile.InstalledVersion{},
			},

			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tailpipeDir, er := filepath.Abs(tt.args.configPath)
			if er != nil {
				t.Errorf("failed to build absolute config filepath from %s", tt.args.configPath)
			}
			// set app_specific.InstallDir
			app_specific.InstallDir = tailpipeDir

			tailpipeConfig, err := parseTailpipeConfig(tt.args.configPath)
			if (err.Error != nil) != tt.wantErr {
				t.Errorf("LoadTailpipeConfig() error = %v, wantErr %v", err.Error, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(tailpipeConfig, tt.want) {
				t.Errorf("LoadTailpipeConfig() = %v, want %v", tailpipeConfig, tt.want)
			}
		})
	}
}
