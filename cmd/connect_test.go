package cmd

import (
	"strings"
	"testing"

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

	// without them, only the attach is left
	got := buildInitScript(commands[2:])
	if strings.Contains(strings.ToLower(got), "install") {
		t.Errorf("script still has install lines: %q", got)
	}
	if got != "-- attach to ducklake database\nattach 'x';\n\n" {
		t.Errorf("unexpected script: %q", got)
	}
}
