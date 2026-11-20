package logger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/turbot/pipe-fittings/v2/app_specific"
	"github.com/turbot/tailpipe/internal/constants"
)

func TestTailpipeLogger_WritesDebugLogToFile(t *testing.T) {
	app_specific.SetAppSpecificEnvVarKeys("TAILPIPE_")
	logFile := filepath.Join(t.TempDir(), "sub", "tailpipe.log")
	if err := os.MkdirAll(filepath.Dir(logFile), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv(app_specific.EnvLogLevel, "DEBUG")
	t.Setenv(constants.EnvLogFile, logFile)

	TailpipeLogger().Debug("hello from the test", "k", "v")
	// a second logger appends rather than truncating
	TailpipeLogger().Info("second line")

	b, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("log file not written: %v", err)
	}
	got := string(b)
	if !strings.Contains(got, "hello from the test") || !strings.Contains(got, "second line") {
		t.Fatalf("log file missing entries (appended?): %q", got)
	}
}

// a log file that cannot be opened must not stop logging or the command
func TestTailpipeLogger_UnwritableLogFileFallsBackToStderr(t *testing.T) {
	app_specific.SetAppSpecificEnvVarKeys("TAILPIPE_")
	t.Setenv(app_specific.EnvLogLevel, "DEBUG")
	t.Setenv(constants.EnvLogFile, filepath.Join(t.TempDir(), "missing-dir", "tailpipe.log"))

	// redirect stderr to a file so we can see the warning
	r, err := os.CreateTemp(t.TempDir(), "stderr")
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stderr
	os.Stderr = r
	defer func() { os.Stderr = orig }()

	l := TailpipeLogger()
	l.Debug("still logging")

	b, _ := os.ReadFile(r.Name())
	got := string(b)
	if !strings.Contains(got, "could not open TAILPIPE_LOG_FILE") || !strings.Contains(got, "still logging") {
		t.Fatalf("expected warning and log on stderr, got %q", got)
	}
}

func TestTailpipeLogger_LevelOffWritesNothingToFile(t *testing.T) {
	app_specific.SetAppSpecificEnvVarKeys("TAILPIPE_")
	logFile := filepath.Join(t.TempDir(), "tailpipe.log")
	t.Setenv(app_specific.EnvLogLevel, "off")
	t.Setenv(constants.EnvLogFile, logFile)

	TailpipeLogger().Error("nope")

	if _, err := os.Stat(logFile); err == nil {
		t.Fatal("log file created although logging is off")
	}
}
