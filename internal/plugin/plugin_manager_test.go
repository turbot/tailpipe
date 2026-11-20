package plugin

import (
	"testing"

	"github.com/turbot/pipe-fittings/v2/versionfile"
)

func TestCorePluginSatisfiesConstraint(t *testing.T) {
	const fullName = "hub.tailpipe.io/plugins/turbot/core@latest"
	const constraint = ">=0.2.0"

	tests := []struct {
		name          string
		plugins       map[string]*versionfile.InstalledVersion
		wantSatisfied bool
		wantAction    string
		wantErr       bool
	}{
		{
			name:       "core plugin missing from the versions record is reinstalled, not a panic",
			plugins:    map[string]*versionfile.InstalledVersion{},
			wantAction: "Installing",
		},
		{
			name: "record holds only other plugins",
			plugins: map[string]*versionfile.InstalledVersion{
				"hub.tailpipe.io/plugins/turbot/aws@latest": {Version: "1.0.0"},
			},
			wantAction: "Installing",
		},
		{
			name:       "nil plugins map",
			plugins:    nil,
			wantAction: "Installing",
		},
		{
			name:       "nil entry",
			plugins:    map[string]*versionfile.InstalledVersion{fullName: nil},
			wantAction: "Installing",
		},
		{
			name:          "version satisfies the constraint",
			plugins:       map[string]*versionfile.InstalledVersion{fullName: {Version: "0.3.0"}},
			wantSatisfied: true,
		},
		{
			name:          "local version is accepted",
			plugins:       map[string]*versionfile.InstalledVersion{fullName: {Version: "local"}},
			wantSatisfied: true,
		},
		{
			name:       "version too old is updated",
			plugins:    map[string]*versionfile.InstalledVersion{fullName: {Version: "0.1.0"}},
			wantAction: "Updating",
		},
		{
			name:    "unparseable version is an error",
			plugins: map[string]*versionfile.InstalledVersion{fullName: {Version: "not-a-version"}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pv := &versionfile.PluginVersionFile{Plugins: tt.plugins}
			satisfied, action, err := corePluginSatisfiesConstraint(pv, fullName, constraint)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if satisfied != tt.wantSatisfied {
				t.Errorf("satisfied = %v, want %v", satisfied, tt.wantSatisfied)
			}
			if action != tt.wantAction {
				t.Errorf("action = %q, want %q", action, tt.wantAction)
			}
		})
	}
}
