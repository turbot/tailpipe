package logger

import (
	"fmt"
	"github.com/turbot/pipe-fittings/v2/app_specific"
	"github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/sanitize"
	tpconstants "github.com/turbot/tailpipe/internal/constants"
	"io"
	"log/slog"
	"os"
	"strings"
)

func Initialize() {
	logger := TailpipeLogger()
	slog.SetDefault(logger)
}

// TailpipeLogger returns a logger that sanitizes log entries and writes them to stderr,
// or to the file named by TAILPIPE_LOG_FILE if that is set (at debug level, unless TAILPIPE_LOG_LEVEL says otherwise)
func TailpipeLogger() *slog.Logger {
	level := getLogLevel()
	if level == constants.LogLevelOff {
		return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
	}

	handlerOptions := &slog.HandlerOptions{
		Level: level,

		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			sanitized := sanitize.Instance.SanitizeKeyValue(a.Key, a.Value.Any())

			return slog.Attr{
				Key:   a.Key,
				Value: slog.AnyValue(sanitized),
			}
		},
	}

	return slog.New(slog.NewJSONHandler(logOutput(), handlerOptions)).With("source", "cli")
}

// logOutput returns the writer for the log: the file named by TAILPIPE_LOG_FILE (appended to, created if needed),
// or stderr when it is not set. If the file cannot be opened, say so on stderr and fall back to it.
// (The file is left open for the life of the process.)
func logOutput() io.Writer {
	path := os.Getenv(tpconstants.EnvLogFile)
	if path == "" {
		return os.Stderr
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600) //nolint:gosec // path is set by the user, to a file they want written
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not open %s %q, logging to stderr: %s\n", tpconstants.EnvLogFile, path, err)
		return os.Stderr
	}
	return f
}

func getLogLevel() slog.Leveler {
	levelEnv := os.Getenv(app_specific.EnvLogLevel)
	// a log file with no level set logs at debug: the file is where a person (or CI) looks for detail, and this
	// avoids setting TAILPIPE_LOG_LEVEL, which the plugins read as well
	if levelEnv == "" && os.Getenv(tpconstants.EnvLogFile) != "" {
		return slog.LevelDebug
	}

	switch strings.ToLower(levelEnv) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "off":
		return constants.LogLevelOff
	default:
		return constants.LogLevelOff
	}
}
