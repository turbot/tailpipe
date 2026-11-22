package metaquery

import (
	"testing"

	"github.com/stretchr/testify/require"
	pluginsdk "github.com/turbot/pipe-fittings/v2/plugin"
	"github.com/turbot/pipe-fittings/v2/versionfile"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
)

func mustVersion(t *testing.T, v string) *pluginsdk.PluginVersionString {
	t.Helper()
	pv, err := pluginsdk.NewPluginVersionString(v)
	require.NoError(t, err)
	return pv
}

func TestGetPluginForTable(t *testing.T) {
	versions := map[string]*versionfile.InstalledVersion{
		"hub.tailpipe.io/plugins/turbot/aws@latest": {Metadata: map[string][]string{"tables": {"aws_cloudtrail_log"}}},
		// a plugin whose table does not start with the plugin name
		"hub.tailpipe.io/plugins/turbot/azure@latest": {Metadata: map[string][]string{"tables": {"azure_activity_log", "msgraph_signin"}}},
		// a plugin whose name is a prefix of another's (the old prefix match could pick either)
		"hub.tailpipe.io/plugins/turbot/a@latest": {Metadata: map[string][]string{"tables": {"a_thing"}}},
	}
	installed := map[string]*pluginsdk.PluginVersionString{
		"turbot/aws":   mustVersion(t, "0.1.0"),
		"turbot/azure": mustVersion(t, "0.2.0"),
		"turbot/a":     mustVersion(t, "0.3.0"),
	}
	custom := map[string]*config.Table{"my_custom": {}}

	t.Run("name starts with plugin name", func(t *testing.T) {
		require.Equal(t, "aws@0.1.0", getPluginForTable("aws_cloudtrail_log", custom, versions, installed))
	})
	t.Run("name does not start with plugin name", func(t *testing.T) {
		require.Equal(t, "azure@0.2.0", getPluginForTable("msgraph_signin", custom, versions, installed))
	})
	t.Run("short plugin name that prefixes another is not confused", func(t *testing.T) {
		require.Equal(t, "a@0.3.0", getPluginForTable("a_thing", custom, versions, installed))
		require.Equal(t, "azure@0.2.0", getPluginForTable("azure_activity_log", custom, versions, installed))
	})
	t.Run("custom table uses core plugin", func(t *testing.T) {
		core := map[string]*pluginsdk.PluginVersionString{"turbot/core": mustVersion(t, "0.4.0")}
		require.Equal(t, "core@0.4.0", getPluginForTable("my_custom", custom, versions, core))
		require.Contains(t, constants.CorePluginInstallStream(), "core@")
	})
	t.Run("plugin not installed", func(t *testing.T) {
		require.Equal(t, "", getPluginForTable("msgraph_signin", custom, versions, map[string]*pluginsdk.PluginVersionString{}))
	})
	t.Run("unknown table", func(t *testing.T) {
		require.Equal(t, "", getPluginForTable("nothing_here", custom, versions, installed))
	})
}
