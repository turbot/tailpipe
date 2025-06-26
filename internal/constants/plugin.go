package constants

const (
	CorePluginName       = "core"
	CorePluginFullName   = "hub.tailpipe.io/plugins/turbot/core@latest"
	MinCorePluginVersion = "v0.2.7"
	PinCorePluginVersion = "" // When set, this will override MinCorePluginVersion
	CorePluginBaseName   = "hub.tailpipe.io/plugins/turbot/core"
	// TailpipeHubOCIBase is the tailpipe hub URL
	TailpipeHubOCIBase = "hub.tailpipe.io/"
	// BaseImageRef is the prefix for all tailpipe plugin images
	BaseImageRef = "ghcr.io/turbot/tailpipe"
)
