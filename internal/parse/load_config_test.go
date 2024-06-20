package parse

import (
	"github.com/turbot/tailpipe/internal/config"
	"reflect"
	"testing"
)

func TestLoadTailpipeConfig(t *testing.T) {
	type args struct {
		configPath string
	}
	tests := []struct {
		name    string
		args    args
		want    *config.TailpipeConfig
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadTailpipeConfig(tt.args.configPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadTailpipeConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LoadTailpipeConfig() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseConfig(t *testing.T) {
	type args struct {
		parseCtx *ConfigParseContext
	}
	tests := []struct {
		name    string
		args    args
		want    *config.TailpipeConfig
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseConfig(tt.args.parseCtx)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseConfig() got = %v, want %v", got, tt.want)
			}
		})
	}
}
