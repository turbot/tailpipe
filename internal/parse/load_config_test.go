package parse

import (
	"path/filepath"
	"reflect"
	"sort"
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

func equalPluginVersions(left, right map[string]*versionfile.InstalledVersion) (bool, string) {
	if (left == nil) != (right == nil) {
		return false, "PluginVersions presence mismatch"
	}
	if left == nil {
		return true, ""
	}
	if len(left) != len(right) {
		return false, fmt.Sprintf("PluginVersions length mismatch: got %d want %d", len(left), len(right))
	}
	for k, v := range left {
		wv, ok := right[k]
		if !ok {
			return false, fmt.Sprintf("PluginVersions missing key '%s' in want", k)
		}
		if (v == nil) != (wv == nil) {
			return false, fmt.Sprintf("PluginVersions['%s'] presence mismatch", k)
		}
		if v != nil {
			if v.Name != wv.Name || v.Version != wv.Version || v.ImageDigest != wv.ImageDigest || v.BinaryDigest != wv.BinaryDigest || v.BinaryArchitecture != wv.BinaryArchitecture || v.InstalledFrom != wv.InstalledFrom || v.StructVersion != wv.StructVersion {
				return false
			}
			if (v.Metadata == nil) != (wv.Metadata == nil) {
				return false
			}
			if v.Metadata != nil {
				if len(v.Metadata) != len(wv.Metadata) {
					return false
				}
				for mk, ma := range v.Metadata {
					mb, ok := wv.Metadata[mk]
					if !ok || len(ma) != len(mb) {
						return false
					}
					maCopy, mbCopy := append([]string(nil), ma...), append([]string(nil), mb...)
					sort.Strings(maCopy)
					sort.Strings(mbCopy)
					for i := range maCopy {
						if maCopy[i] != mbCopy[i] {
							return false
						}
					}
				}
			}
		}
	}
	return true
}

func equalConnections(left, right map[string]*config.TailpipeConnection) (bool, string) {
	if (left == nil) != (right == nil) {
		return false, "Connections presence mismatch"
	}
	if left == nil {
		return true, ""
	}
	if len(left) != len(right) {
		return false, fmt.Sprintf("Connections length mismatch: got %d want %d", len(left), len(right))
	}
	for k, conn := range left {
		wconn, ok := right[k]
		if !ok {
			return false, fmt.Sprintf("Connections missing key '%s' in want", k)
		}
		if (conn == nil) != (wconn == nil) {
			return false, fmt.Sprintf("Connections['%s'] presence mismatch", k)
		}
		if conn != nil {
			if conn.HclResourceImpl.FullName != wconn.HclResourceImpl.FullName || conn.HclResourceImpl.ShortName != wconn.HclResourceImpl.ShortName || conn.HclResourceImpl.UnqualifiedName != wconn.HclResourceImpl.UnqualifiedName || conn.HclResourceImpl.BlockType != wconn.HclResourceImpl.BlockType {
				return false
			}
			if conn.Plugin != wconn.Plugin {
				return false
			}
			// HclRange: presence mismatch fails; when both present, use DeepEqual
			zero := hclhelpers.Range{}
			connZero := conn.HclRange == zero
			wconnZero := wconn.HclRange == zero
			if connZero != wconnZero {
				return false
			}
			if !connZero && !wconnZero {
				if !reflect.DeepEqual(conn.HclRange, wconn.HclRange) {
					gr, wr := conn.HclRange, wconn.HclRange
					return false, fmt.Sprintf("Connections['%s'].HclRange mismatch: got %s:(%d,%d,%d)-(%d,%d,%d) want %s:(%d,%d,%d)-(%d,%d,%d)", k,
						gr.Filename, gr.Start.Line, gr.Start.Column, gr.Start.Byte, gr.End.Line, gr.End.Column, gr.End.Byte,
						wr.Filename, wr.Start.Line, wr.Start.Column, wr.Start.Byte, wr.End.Line, wr.End.Column, wr.End.Byte)
				}
			}
		}
	}
	return true
}

func equalCustomTables(left, right map[string]*config.Table) (bool, string) {
	if (left == nil) != (right == nil) {
		return false, "CustomTables presence mismatch"
	}
	if left == nil {
		return true, ""
	}
	if len(left) != len(right) {
		return false, fmt.Sprintf("CustomTables length mismatch: got %d want %d", len(left), len(right))
	}
	for k, ct := range left {
		wct, ok := right[k]
		if !ok {
			return false, fmt.Sprintf("CustomTables missing key '%s' in want", k)
		}
		if (ct == nil) != (wct == nil) {
			return false, fmt.Sprintf("CustomTables['%s'] presence mismatch", k)
		}
		if ct != nil {
			if ct.HclResourceImpl.FullName != wct.HclResourceImpl.FullName || ct.HclResourceImpl.ShortName != wct.HclResourceImpl.ShortName || ct.HclResourceImpl.UnqualifiedName != wct.HclResourceImpl.UnqualifiedName || ct.HclResourceImpl.BlockType != wct.HclResourceImpl.BlockType {
				return false
			}
			if ct.DefaultSourceFormat != nil && wct.DefaultSourceFormat != nil {
				if ct.DefaultSourceFormat.Type != wct.DefaultSourceFormat.Type || ct.DefaultSourceFormat.PresetName != wct.DefaultSourceFormat.PresetName {
					return false
				}
				if ct.DefaultSourceFormat.HclResourceImpl.FullName != wct.DefaultSourceFormat.HclResourceImpl.FullName || ct.DefaultSourceFormat.HclResourceImpl.ShortName != wct.DefaultSourceFormat.HclResourceImpl.ShortName || ct.DefaultSourceFormat.HclResourceImpl.UnqualifiedName != wct.DefaultSourceFormat.HclResourceImpl.UnqualifiedName || ct.DefaultSourceFormat.HclResourceImpl.BlockType != wct.DefaultSourceFormat.HclResourceImpl.BlockType {
					return false
				}
			}
			if len(ct.Columns) != len(wct.Columns) {
				return false
			}
			for i := range ct.Columns {
				ac, bc := ct.Columns[i], wct.Columns[i]
				if ac.Name != bc.Name {
					return false
				}
				if ac.Type != nil && bc.Type != nil && *ac.Type != *bc.Type {
					return false
				}
				if ac.Source != nil && bc.Source != nil && *ac.Source != *bc.Source {
					return false
				}
				if ac.Description != nil && bc.Description != nil && *ac.Description != *bc.Description {
					return false
				}
				if ac.Required != nil && bc.Required != nil && *ac.Required != *bc.Required {
					return false
				}
				if ac.NullIf != nil && bc.NullIf != nil && *ac.NullIf != *bc.NullIf {
					return false
				}
				if ac.Transform != nil && bc.Transform != nil && *ac.Transform != *bc.Transform {
					return false
				}
			}
			// map_fields
			mfA := append([]string(nil), ct.MapFields...)
			if len(mfA) == 0 {
				mfA = []string{"*"}
			}
			mfB := append([]string(nil), wct.MapFields...)
			if len(mfB) == 0 {
				mfB = []string{"*"}
			}
			sort.Strings(mfA)
			sort.Strings(mfB)
			if len(mfA) != len(mfB) {
				return false
			}
			for i := range mfA {
				if mfA[i] != mfB[i] {
					return false
				}
			}
			if ct.NullIf != wct.NullIf {
				return false
			}
		}
	}
	return true
}

func equalFormats(left, right map[string]*config.Format) (bool, string) {
	if (left == nil) != (right == nil) {
		return false, "Formats presence mismatch"
	}
	if left == nil {
		return true, ""
	}
	if len(left) != len(right) {
		return false, fmt.Sprintf("Formats length mismatch: got %d want %d", len(left), len(right))
	}
	for k, f := range left {
		wf, ok := right[k]
		if !ok {
			return false, fmt.Sprintf("Formats missing key '%s' in want", k)
		}
		if (f == nil) != (wf == nil) {
			return false, fmt.Sprintf("Formats['%s'] presence mismatch", k)
		}
		if f != nil {
			if f.Type != wf.Type {
				return false
			}
			if f.HclResourceImpl.FullName != wf.HclResourceImpl.FullName || f.HclResourceImpl.ShortName != wf.HclResourceImpl.ShortName || f.HclResourceImpl.UnqualifiedName != wf.HclResourceImpl.UnqualifiedName || f.HclResourceImpl.BlockType != wf.HclResourceImpl.BlockType {
				return false
			}
			if f.PresetName != "" && wf.PresetName != "" && f.PresetName != wf.PresetName {
				return false
			}
		}
	}
	return true
}

func equalPartitions(left, right map[string]*config.Partition) (bool, string) {
	if (left == nil) != (right == nil) {
		return false, "Partitions presence mismatch"
	}
	if left == nil {
		return true, ""
	}
	if len(left) != len(right) {
		return false, fmt.Sprintf("Partitions length mismatch: got %d want %d", len(left), len(right))
	}
	for k, p := range left {
		wp, ok := right[k]
		if !ok {
			return false, fmt.Sprintf("Partitions missing key '%s' in want", k)
		}
		if (p == nil) != (wp == nil) {
			return false, fmt.Sprintf("Partitions['%s'] presence mismatch", k)
		}
		if p != nil {
			// identity
			if p.HclResourceImpl.FullName != wp.HclResourceImpl.FullName || p.HclResourceImpl.ShortName != wp.HclResourceImpl.ShortName || p.HclResourceImpl.UnqualifiedName != wp.HclResourceImpl.UnqualifiedName || p.HclResourceImpl.BlockType != wp.HclResourceImpl.BlockType {
				return false
			}
			if p.TableName != wp.TableName {
				return false
			}
			// source
			if p.Source.Type != wp.Source.Type {
				return false
			}
			if (p.Source.Connection == nil) != (wp.Source.Connection == nil) {
				return false
			}
			if p.Source.Connection != nil && wp.Source.Connection != nil {
				if p.Source.Connection.HclResourceImpl.UnqualifiedName != wp.Source.Connection.HclResourceImpl.UnqualifiedName {
					return false
				}
			}
			if (p.Source.Format == nil) != (wp.Source.Format == nil) {
				return false
			}
			if p.Source.Format != nil && wp.Source.Format != nil {
				pf, of := p.Source.Format, wp.Source.Format
				if pf.Type != of.Type || pf.PresetName != of.PresetName {
					return false
				}
				if pf.HclResourceImpl.FullName != of.HclResourceImpl.FullName || pf.HclResourceImpl.ShortName != of.HclResourceImpl.ShortName || pf.HclResourceImpl.UnqualifiedName != of.HclResourceImpl.UnqualifiedName || pf.HclResourceImpl.BlockType != of.HclResourceImpl.BlockType {
					return false
				}
			}
			if (p.Source.Config == nil) != (wp.Source.Config == nil) {
				return false
			}
			if p.Source.Config != nil && p.Source.Config.Range != wp.Source.Config.Range {
				return false
			}
			// partition config
			if !(len(p.Config) == 0 && len(wp.Config) == 0) {
				if string(p.Config) != string(wp.Config) || p.ConfigRange != wp.ConfigRange {
					return false
				}
			}
			if p.Filter != wp.Filter || p.TpIndexColumn != wp.TpIndexColumn {
				return false
			}
			if (p.CustomTable == nil) != (wp.CustomTable == nil) {
				return false
			}
			if p.CustomTable != nil && wp.CustomTable != nil {
				if !reflect.DeepEqual(p.CustomTable, wp.CustomTable) {
					return false
				}
			}
			// plugin
			if p.Plugin != nil && wp.Plugin != nil {
				if p.Plugin.Instance != wp.Plugin.Instance || p.Plugin.Alias != wp.Plugin.Alias || p.Plugin.Plugin != wp.Plugin.Plugin {
					return false
				}
			}
		}
	}
	return true
}

func equalTailpipeConfig(left, right *config.TailpipeConfig) (bool, string) {
	if left == nil || right == nil {
		if left == right {
			return true, ""
		}
		return false, "nil vs non-nil TailpipeConfig"
	}
	if ok, msg := equalPluginVersions(left.PluginVersions, right.PluginVersions); !ok {
		return false, msg
	}
	if ok, msg := equalPartitions(left.Partitions, right.Partitions); !ok {
		return false, msg
	}
	if ok, msg := equalConnections(left.Connections, right.Connections); !ok {
		return false, msg
	}
	if ok, msg := equalCustomTables(left.CustomTables, right.CustomTables); !ok {
		return false, msg
	}
	if ok, msg := equalFormats(left.Formats, right.Formats); !ok {
		return false, msg
	}
	return true
}

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
						HclResourceImpl: modconfig.HclResourceImpl{
							FullName:        "aws_cloudtrail_log.cloudtrail_logs",
							ShortName:       "cloudtrail_logs",
							UnqualifiedName: "aws_cloudtrail_log.cloudtrail_logs",
							DeclRange: hcl.Range{
								Filename: "test_data/static_table_config/resources.tpc",
								Start:    hcl.Pos{Line: 3, Column: 50, Byte: 103},
								End:      hcl.Pos{Line: 9, Column: 2, Byte: 252},
							},
							BlockType: "partition",
						},
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
						HclResourceImpl: modconfig.HclResourceImpl{
							FullName:        "aws_vpc_flow_log.flow_logs",
							ShortName:       "flow_logs",
							UnqualifiedName: "aws_vpc_flow_log.flow_logs",
							DeclRange: hcl.Range{
								Filename: "test_data/static_table_config/resources.tpc",
								Start:    hcl.Pos{Line: 12, Column: 42, Byte: 351},
								End:      hcl.Pos{Line: 22, Column: 2, Byte: 636},
							},
							BlockType: "partition",
						},
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
									End:      hcl.Pos{Line: 20, Column: 34, Byte: 628},
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
							Format: &config.Format{
								Type:       "delimited",
								PresetName: "",
								HclResourceImpl: modconfig.HclResourceImpl{
									FullName:        "delimited.csv_logs",
									ShortName:       "csv_logs",
									UnqualifiedName: "delimited.csv_logs",
									BlockType:       "format",
								},
							},
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
		{
			name: "invalid path",
			args: args{
				configPath: "test_data/does_not_exist",
			},
			want: &config.TailpipeConfig{
				PluginVersions: map[string]*versionfile.InstalledVersion{},
				Partitions:     map[string]*config.Partition{},
				Connections:    map[string]*config.TailpipeConnection{},
				CustomTables:   map[string]*config.Table{},
				Formats:        map[string]*config.Format{},
			},
			wantErr: false,
		},
		{
			name: "malformed hcl",
			args: args{
				configPath: "test_data/malformed_config",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "invalid partition labels",
			args: args{
				configPath: "test_data/invalid_partition_labels",
			},
			want:    nil,
			wantErr: true,
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

			// normalize raw HCL bytes for static tables (Source.Config.Hcl differs by whitespace/order)
			// if tt.name == "static tables" {
			// 	for _, p := range tailpipeConfig.Partitions {
			// 		if p != nil && p.Source.Config != nil {
			// 			p.Source.Config.Hcl = nil
			// 		}
			// 	}
			// 	for _, p := range tt.want.Partitions {
			// 		if p != nil && p.Source.Config != nil {
			// 			p.Source.Config.Hcl = nil
			// 		}
			// 	}
			// }

			// use TailpipeConfig.EqualConfig for all cases (ignores Source.Config.Hcl differences)
			if !equalTailpipeConfig(tailpipeConfig, tt.want) {
				t.Errorf("TailpipeConfig.EqualConfig() mismatch")
				return
			}

			// DeepEqual intentionally skipped EqualConfig is added instead
			// if !reflect.DeepEqual(tailpipeConfig, tt.want) {
			// 	t.Errorf("LoadTailpipeConfig() = %v, want %v", tailpipeConfig, tt.want)
			// }
		})
	}
}
