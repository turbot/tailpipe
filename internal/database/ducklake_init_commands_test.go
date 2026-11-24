package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcboeker/go-duckdb/v2"
	"github.com/turbot/pipe-fittings/v2/app_specific"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/workspace_profile"
	"github.com/turbot/tailpipe/internal/config"
)

// useTestWorkspace points the global workspace profile at a folder owned by the test
func useTestWorkspace(t *testing.T) {
	t.Helper()
	prevDir, prevProfile := app_specific.InstallDir, config.GlobalWorkspaceProfile
	app_specific.InstallDir = t.TempDir()
	config.GlobalWorkspaceProfile = &workspace_profile.TailpipeWorkspaceProfile{ProfileName: "test"}
	t.Cleanup(func() {
		app_specific.InstallDir, config.GlobalWorkspaceProfile = prevDir, prevProfile
	})
}

func commandNames(commands []SqlCommand) []string {
	var res []string
	for _, c := range commands {
		res = append(res, c.Command)
	}
	return res
}

func countInstall(commands []SqlCommand) int {
	n := 0
	for _, c := range commands {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(c.Command)), "install") {
			n++
		}
	}
	return n
}

func TestGetDucklakeInitCommands_default(t *testing.T) {
	useTestWorkspace(t)
	for _, readonly := range []bool{true, false} {
		got := GetDucklakeInitCommands(readonly)
		// unchanged from before the skip-install option existed: two installs, then the attach
		if len(got) != 3 {
			t.Fatalf("readonly=%v: want 3 commands, got %d: %v", readonly, len(got), commandNames(got))
		}
		if got[0] != (SqlCommand{Description: "install sqlite extension", Command: "install sqlite"}) {
			t.Errorf("readonly=%v: unexpected first command %+v", readonly, got[0])
		}
		if got[1] != (SqlCommand{Description: "install ducklake extension", Command: "install ducklake;"}) {
			t.Errorf("readonly=%v: unexpected second command %+v", readonly, got[1])
		}
		if got[2].Description != "attach to ducklake database" || !strings.HasPrefix(got[2].Command, "attach 'ducklake:sqlite:") {
			t.Errorf("readonly=%v: unexpected third command %+v", readonly, got[2])
		}
		// the explicit option set to false gives the very same commands
		again := GetDucklakeInitCommandsWithOptions(readonly, false)
		if fmt.Sprint(again) != fmt.Sprint(got) {
			t.Errorf("readonly=%v: skipInstall=false differs from the default:\n%v\n%v", readonly, again, got)
		}
	}
}

func TestGetDucklakeInitCommandsWithOptions_skipInstall(t *testing.T) {
	useTestWorkspace(t)
	for _, readonly := range []bool{true, false} {
		withInstall := GetDucklakeInitCommands(readonly)
		got := GetDucklakeInitCommandsWithOptions(readonly, true)
		if n := countInstall(got); n != 0 {
			t.Errorf("readonly=%v: want no install commands, got %d: %v", readonly, n, commandNames(got))
		}
		// only the install commands are left out: the attach is the same
		want := withInstall[len(withInstall)-1]
		if len(got) != 1 || got[0] != want {
			t.Errorf("readonly=%v: want only the attach command %+v, got %+v", readonly, want, got)
		}
		if readonly != strings.Contains(got[0].Command, "READ_ONLY") {
			t.Errorf("readonly=%v: attach does not honour the read only option: %s", readonly, got[0].Command)
		}
	}
}

// Test_skipInstall_lockedDuckDb runs the script in a DuckDB that has loaded both extensions and been locked
// the way an app with permitted root paths locks it, and checks that the script without install lines attaches
// the data and returns rows.
func Test_skipInstall_lockedDuckDb(t *testing.T) {
	useTestWorkspace(t)
	extDir := t.TempDir()
	dataDir := config.GlobalWorkspaceProfile.GetDataDir()
	if err := os.MkdirAll(dataDir, 0755); err != nil { //nolint:gosec // test code
		t.Fatal(err)
	}

	open := func(path string) *sql.DB {
		connector, err := duckdb.NewConnector(path, nil)
		if err != nil {
			t.Fatalf("failed to open duckdb: %v", err)
		}
		db := sql.OpenDB(connector)
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	exec := func(db *sql.DB, q string) error {
		_, err := db.Exec(q)
		return err
	}

	// create the ducklake with a table and a row, using a writeable connection
	setup := open("")
	if err := exec(setup, fmt.Sprintf("set extension_directory = '%s'", extDir)); err != nil {
		t.Fatal(err)
	}
	for _, ext := range []string{"sqlite", "ducklake"} {
		if err := exec(setup, fmt.Sprintf("install %s; load %s", ext, ext)); err != nil {
			t.Skipf("cannot install the %s extension here (no network?): %v", ext, err)
		}
	}
	for _, c := range GetDucklakeInitCommandsWithOptions(false, true) {
		if err := exec(setup, c.Command); err != nil {
			t.Fatalf("failed to attach writeable ducklake: %v", err)
		}
	}
	for _, q := range []string{
		"create table " + pconstants.DuckLakeCatalog + ".t as select 42 as answer",
	} {
		if err := exec(setup, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	_ = setup.Close()

	// a new DuckDB: load both extensions, then lock it
	lock := func() *sql.DB {
		db := open("")
		if err := exec(db, fmt.Sprintf("set extension_directory = '%s'", extDir)); err != nil {
			t.Fatal(err)
		}
		for _, ext := range []string{"sqlite", "ducklake"} {
			if err := exec(db, fmt.Sprintf("install %s; load %s", ext, ext)); err != nil {
				t.Fatal(err)
			}
		}
		for _, q := range []string{
			fmt.Sprintf("set allowed_directories = ['%s', '%s']", filepath.ToSlash(dataDir), filepath.ToSlash(extDir)),
			"set enable_external_access = false",
			"set lock_configuration = true",
		} {
			if err := exec(db, q); err != nil {
				t.Fatalf("lock %q: %v", q, err)
			}
		}
		return db
	}

	t.Run("the lock is in force", func(t *testing.T) {
		db := lock()
		if err := exec(db, "set enable_external_access = true"); err == nil {
			t.Fatal("expected the locked duckdb to refuse a settings change")
		}
	})

	t.Run("without install lines it attaches and returns rows", func(t *testing.T) {
		db := lock()
		for _, c := range GetDucklakeInitCommandsWithOptions(true, true) {
			if err := exec(db, c.Command); err != nil {
				t.Fatalf("command %q failed on a locked duckdb: %v", c.Command, err)
			}
		}
		var answer int
		if err := db.QueryRow("select answer from " + pconstants.DuckLakeCatalog + ".t").Scan(&answer); err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if answer != 42 {
			t.Errorf("want 42, got %d", answer)
		}
	})
}
