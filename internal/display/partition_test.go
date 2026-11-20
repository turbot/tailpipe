package display

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/turbot/pipe-fittings/v2/versionfile"
	"github.com/turbot/tailpipe/internal/config"
)

func TestNewPartitionResource_Configured(t *testing.T) {
	versions := map[string]*versionfile.InstalledVersion{
		"hub.tailpipe.io/plugins/turbot/aws@latest": {Metadata: map[string][]string{"tables": {"aws_cloudtrail_log"}}},
	}
	removed := config.NewUnconfiguredPartition("aws_cloudtrail_log", "removed", versions)

	res := NewPartitionResource(removed)
	require.False(t, res.Configured)
	require.Equal(t, "aws_cloudtrail_log.removed", res.Name)
	require.Equal(t, "aws_cloudtrail_log", res.table)
	require.Equal(t, "removed", res.partition)

	// the list row says it is not configured
	require.Equal(t, "no", res.GetListData().Fields["configured"].Value)
	require.Equal(t, "no", res.GetShowData().Fields["configured"].Value)

	kept := &config.Partition{TableName: "aws_cloudtrail_log", Plugin: removed.Plugin}
	kept.ShortName = "kept"
	kept.UnqualifiedName = "aws_cloudtrail_log.kept"
	keptRes := NewPartitionResource(kept)
	require.True(t, keptRes.Configured)
	require.Equal(t, "yes", keptRes.GetListData().Fields["configured"].Value)
}

func TestPluginSetPartitions_IncludesUnconfigured(t *testing.T) {
	const pluginName = "hub.tailpipe.io/plugins/turbot/aws@latest"
	versions := map[string]*versionfile.InstalledVersion{
		pluginName: {Metadata: map[string][]string{"tables": {"aws_cloudtrail_log"}}},
	}
	kept := config.NewUnconfiguredPartition("aws_cloudtrail_log", "kept", versions)
	kept.Unconfigured = false
	removed := config.NewUnconfiguredPartition("aws_cloudtrail_log", "removed", versions)
	other := config.NewUnconfiguredPartition("gcp_audit_log", "removed", versions)
	partitions := []*config.Partition{kept, removed, other}

	t.Run("plugin list", func(t *testing.T) {
		d := &PluginListDetails{Name: pluginName, Version: "0.1.0"}
		d.setPartitions(partitions)
		require.Equal(t, []string{"aws_cloudtrail_log.kept", "aws_cloudtrail_log.removed"}, d.Partitions)
	})
	t.Run("plugin show", func(t *testing.T) {
		r := &PluginResource{Name: pluginName}
		r.setPartitions(partitions)
		require.Equal(t, []string{"aws_cloudtrail_log.kept", "aws_cloudtrail_log.removed"}, r.Partitions)
	})
}

func TestTableSetPartitions_IncludesUnconfigured(t *testing.T) {
	kept := config.NewUnconfiguredPartition("aws_cloudtrail_log", "kept", nil)
	kept.Unconfigured = false
	removed := config.NewUnconfiguredPartition("aws_cloudtrail_log", "removed", nil)
	other := config.NewUnconfiguredPartition("gcp_audit_log", "removed", nil)

	r := &TableResource{Name: "aws_cloudtrail_log"}
	r.setPartitions([]*config.Partition{removed, other, kept})
	require.Equal(t, []string{"kept", "removed"}, r.Partitions)
}
