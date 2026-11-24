package cmd

import (
	"strings"
	"testing"

	"github.com/turbot/pipe-fittings/v2/app_specific"
	"github.com/turbot/pipe-fittings/v2/workspace_profile"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
)

func Test_connectCmd_noInstallFlag(t *testing.T) {
	cmd := connectCmd()
	flag := cmd.Flags().Lookup(constants.ArgNoInstall)
	if flag == nil {
		t.Fatalf("connect has no --%s flag", constants.ArgNoInstall)
	}
	if flag.DefValue != "false" {
		t.Errorf("--%s should be off by default, got %q", constants.ArgNoInstall, flag.DefValue)
	}
	if !strings.Contains(cmd.UsageString(), "--"+constants.ArgNoInstall) {
		t.Errorf("--%s is not named in the usage:\n%s", constants.ArgNoInstall, cmd.UsageString())
	}
	if !strings.Contains(cmd.Long, "--"+constants.ArgNoInstall) {
		t.Errorf("--%s is not named in the long help", constants.ArgNoInstall)
	}
}

func Test_buildInitScript(t *testing.T) {
	commands := []database.SqlCommand{
		{Description: "install sqlite extension", Command: "install sqlite"},
		{Description: "install ducklake extension", Command: "install ducklake;"},
		{Description: "attach to ducklake database", Command: "attach 'x'"},
	}

	// with the install commands, the script is exactly what it was before the option existed
	want := "-- install sqlite extension\ninstall sqlite;\n\n" +
		"-- install ducklake extension\ninstall ducklake;;\n\n" +
		"-- attach to ducklake database\nattach 'x';\n\n"
	if got := buildInitScript(commands); got != want {
		t.Errorf("script changed:\ngot:  %q\nwant: %q", got, want)
	}
}

// the script `tailpipe connect --no-install` writes: the commands from the database package, rendered
func Test_buildInitScript_noInstall(t *testing.T) {
	prevDir, prevProfile := app_specific.InstallDir, config.GlobalWorkspaceProfile
	app_specific.InstallDir = t.TempDir()
	config.GlobalWorkspaceProfile = &workspace_profile.TailpipeWorkspaceProfile{ProfileName: "test"}
	t.Cleanup(func() {
		app_specific.InstallDir, config.GlobalWorkspaceProfile = prevDir, prevProfile
	})

	withInstall := buildInitScript(database.GetDucklakeInitCommandsWithOptions(true, false))
	without := buildInitScript(database.GetDucklakeInitCommandsWithOptions(true, true))

	if !strings.Contains(withInstall, "install sqlite;") || !strings.Contains(withInstall, "install ducklake;") {
		t.Errorf("script without the option lost its install lines: %q", withInstall)
	}
	if strings.Contains(strings.ToLower(without), "install") {
		t.Errorf("script still has install lines: %q", without)
	}
	if !strings.Contains(without, "attach 'ducklake:sqlite:") {
		t.Errorf("script lost its attach: %q", without)
	}
	// only the install lines differ
	if !strings.HasSuffix(withInstall, without) {
		t.Errorf("scripts differ by more than the install lines:\n%q\n%q", withInstall, without)
	}
}
