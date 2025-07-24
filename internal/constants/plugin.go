package constants

import (
	"strings"
)

const (

	// MinCorePluginVersion should be set for production releases - it is the minimum version of the core plugin that is required
	MinCorePluginVersion = "v0.2.10"
	// CorePluginVersion may  be set for pre-release versions - it allows us to pin a pre-release version of the core plugin
	// NOTE: they must NOT both be set
	CorePluginVersion = ""
	// TailpipeHubOCIBase is the tailpipe hub URL
	TailpipeHubOCIBase = "hub.tailpipe.io/"

	// BaseImageRef is the prefix for all tailpipe plugin images
	BaseImageRef = "ghcr.io/turbot/tailpipe"
)

// CorePluginRequiredVersionConstraint returns a version constraint for the required core plugin version
// normally we set the core version by setting constants.MinCorePluginVersion
// However if we want ot pin to a specific version (e.g. an rc version) we can set constants.CorePluginVersion instead
// one of constants.CorePluginVersion and constants.MinCorePluginVersion may be set
// if both are set this is a bug
func CorePluginRequiredVersionConstraint() (requiredConstraint string) {
	if CorePluginVersion == "" && MinCorePluginVersion == "" {
		panic("one of constants.CorePluginName or constants.MinCorePluginVersion must be set")
	}
	if CorePluginVersion != "" && MinCorePluginVersion != "" {
		panic("both constants.CorePluginVersion and constants.MinCorePluginVersion are set, this is a bug")
	}
	if MinCorePluginVersion != "" {
		requiredConstraint = ">=" + MinCorePluginVersion
		return requiredConstraint
	}

	// so CorePluginVersion is set - return as-is
	return CorePluginVersion
}

// CorePluginInstallStream returns the plugin stream used to install the core plugin
// under normal circumstances (i.e. if MinCorePluginVersion is set) this is "core@latest"
func CorePluginInstallStream() string {
	var installConstraint string
	if MinCorePluginVersion != "" {
		installConstraint = "latest"
	} else {
		// so CorePluginVersion is set
		// tactical - trim 'v' as installation expects no v
		installConstraint = strings.TrimPrefix(CorePluginVersion, "v")

	}

	return "core@" + installConstraint
}

func CorePluginFullName() string {
	installStream := CorePluginInstallStream()
	return "hub.tailpipe.io/plugins/turbot/" + installStream
}
