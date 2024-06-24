package logger

import (
	"github.com/spf13/viper"
	"github.com/turbot/pipe-fittings/app_specific"
	"github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/sanitize"
	"io"
	"log/slog"
	"os"
	"strings"
)

func Initialize() {
	logger := TailpipeLogger()
	slog.SetDefault(logger)

	// pump in the initial set of logs
	// this will also write out the Execution ID - enabling easy filtering of logs for a single execution
	// we need to do this since all instances will log to a single file and logs will be interleaved
	slog.Info("Tailpipe CLI",
		"app version", viper.GetString("main.version"),
		"log level", os.Getenv(app_specific.EnvLogLevel))
}

// TailpipeLogger returns a logger that writes to stderr and sanitizes log entries
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

	return slog.New(slog.NewJSONHandler(os.Stderr, handlerOptions)).With("source", "cli")
}

func getLogLevel() slog.Leveler {
	levelEnv := os.Getenv(app_specific.EnvLogLevel)

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
