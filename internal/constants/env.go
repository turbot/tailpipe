package constants

const (
	EnvPluginStartTimeout = "TAILPIPE_PLUGIN_START_TIMEOUT"
	// EnvConfigDump is an undocumented variable that is used to test config precedence
	EnvConfigDump = "TAILPIPE_CONFIG_DUMP"
	// EnvLogFile is the path of a file to write the log to (appended to) instead of stderr
	EnvLogFile = "TAILPIPE_LOG_FILE"
)
