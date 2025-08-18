package parse

// TODO enable and fix this test https://github.com/turbot/tailpipe/issues/506
//func TestLoadTailpipeConfig(t *testing.T) {
//	type args struct {
//		configPath string
//		partition  string
//	}
//	tests := []struct {
//		name    string
//		args    args
//		want    *config.TailpipeConfig
//		wantErr bool
//	}{
//		// TODO #testing add more test cases https://github.com/turbot/tailpipe/issues/506
//		{
//			name: "static tables",
//			args: args{
//				configPath: "test_data/static_table_config",
//				partition:  "partition.aws_cloudtrail_log.cloudtrail_logs",
//			},
//			want: &config.TailpipeConfig{
//				PluginVersions: nil,
//				Partitions: map[string]*config.Partition{
//					"partition.aws_cloudtrail_log.cloudtrail_logs": {},
//					"partition.aws_vpc_flow_log.flow_logs": {},
//				},
//			},
//
//			wantErr: false,
//		},
//		{
//			name: "dynamic tables",
//			args: args{
//				configPath: "test_data/custom_table_config",
//				partition:  "partition.aws_cloudtrail_log.cloudtrail_logs",
//			},
//			want: &config.TailpipeConfig{
//				Partitions: map[string]*config.Partition{
//					"my_csv_log.test": {
//						HclResourceImpl: modconfig.HclResourceImpl{
//							FullName:        "partition.my_csv_log.test",
//							ShortName:       "test",
//							UnqualifiedName: "my_csv_log.test",
//							DeclRange: hcl.Range{
//								Filename: "test_data/custom_table_config/resources.tpc",
//								Start: hcl.Pos{
//									Line:   2,
//									Column: 30,
//									Byte:   30,
//								},
//								End: hcl.Pos{
//									Line:   10,
//									Column: 2,
//									Byte:   230,
//								},
//							},
//							BlockType: "partition",
//						},
//						TableName: "my_csv_log",
//						Plugin: &plugin.Plugin{
//							Instance: "custom",
//							Alias:    "custom",
//							Plugin:   "/plugins/turbot/custom@latest",
//						},
//						Source: config.Source{
//							Type: "file_system",
//							Config: &config.HclBytes{
//								Hcl: []byte("extensions = [\".csv\"]\npaths = [\"/Users/kai/tailpipe_data/logs\"]"),
//								Range: hclhelpers.NewRange(hcl.Range{
//									Filename: "test_data/custom_table_config/resources.tpc",
//									Start: hcl.Pos{
//										Line:   4,
//										Column: 9,
//										Byte:   68,
//									},
//									End: hcl.Pos{
//										Line:   5,
//										Column: 30,
//										Byte:   139,
//									},
//								}),
//							},
//						},
//					},
//				},
//				CustomTables: map[string]*config.Table{
//					"my_csv_log": {
//						HclResourceImpl: modconfig.HclResourceImpl{
//							FullName:        "partition.my_csv_log.test",
//							ShortName:       "test",
//							UnqualifiedName: "my_csv_log.test",
//							DeclRange: hcl.Range{
//								Filename: "test_data/custom_table_config/resources.tpc",
//								Start: hcl.Pos{
//									Line:   2,
//									Column: 30,
//									Byte:   30,
//								},
//								End: hcl.Pos{
//									Line:   10,
//									Column: 2,
//									Byte:   230,
//								},
//							},
//							BlockType: "partition",
//						},
//						//Mode: schema.ModePartial,
//						Columns: []config.ColumnSchema{
//							{
//								Name:   "tp_timestamp",
//								Source: utils.ToPointer("time_local"),
//							},
//							{
//								Name:   "tp_index",
//								Source: utils.ToPointer("account_id"),
//							},
//							{
//								Name:   "org_id",
//								Source: utils.ToPointer("org"),
//							},
//							{
//								Name: "user_id",
//								Type: utils.ToPointer("varchar"),
//							},
//						},
//					},
//				},
//			},
//
//			wantErr: false,
//		},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			tailpipeDir, er := filepath.Abs(tt.args.configPath)
//			if er != nil {
//				t.Errorf("failed to build absolute config filepath from %s", tt.args.configPath)
//			}
//			// set app_specific.InstallDir
//			app_specific.InstallDir = tailpipeDir
//
//			tailpipeConfig, err := parseTailpipeConfig(tt.args.configPath)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("LoadTailpipeConfig() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//
//			if !reflect.DeepEqual(tailpipeConfig, tt.want) {
//				t.Errorf("LoadTailpipeConfig() = %v, want %v", tailpipeConfig, tt.want)
//			}
//		})
//	}
//}
