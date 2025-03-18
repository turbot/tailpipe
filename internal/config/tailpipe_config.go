package config

import (
	"fmt"
	"strings"

	"github.com/hashicorp/hcl/v2"

	"github.com/turbot/pipe-fittings/v2/modconfig"
	"github.com/turbot/pipe-fittings/v2/plugin"
	"github.com/turbot/pipe-fittings/v2/versionfile"
	"github.com/turbot/tailpipe/internal/constants"
)

type TailpipeConfig struct {
	// map of partitions, keyed by unqualified name (<partition_type>.<partition_name>)
	Partitions map[string]*Partition

	Connections map[string]*TailpipeConnection

	PluginVersions map[string]*versionfile.InstalledVersion
	CustomTables   map[string]*Table
	Formats        map[string]*Format
}

func NewTailpipeConfig() *TailpipeConfig {
	return &TailpipeConfig{
		Partitions:     make(map[string]*Partition),
		Connections:    make(map[string]*TailpipeConnection),
		PluginVersions: make(map[string]*versionfile.InstalledVersion),
		CustomTables:   make(map[string]*Table),
		Formats:        make(map[string]*Format),
	}
}
func (c *TailpipeConfig) Add(resource modconfig.HclResource) error {
	switch t := resource.(type) {
	case *Partition:
		c.Partitions[t.GetUnqualifiedName()] = t
		return nil
	case *TailpipeConnection:
		c.Connections[t.GetUnqualifiedName()] = t
		return nil
	case *Table:
		c.CustomTables[t.GetUnqualifiedName()] = t
		return nil
	case *Format:
		c.Formats[t.GetUnqualifiedName()] = t
		return nil
	default:
		return fmt.Errorf("unsupported resource type %T", t)
	}
}

func (c *TailpipeConfig) Validate() hcl.Diagnostics {
	var diags hcl.Diagnostics

	for _, partition := range c.Partitions {
		diags = append(diags, partition.Validate()...)
	}
	for _, customTable := range c.CustomTables {
		diags = append(diags, customTable.Validate()...)
	}

	return diags
}

func (c *TailpipeConfig) InitPartitions(v *versionfile.PluginVersionFile) {
	// populate the plugin property for each partition
	for _, partition := range c.Partitions {
		// set the table on the plugin (in case it is a custom table)
		if _, isCustom := c.CustomTables[partition.TableName]; isCustom {
			partition.CustomTable = c.CustomTables[partition.TableName]
		}
		// if the plugin is not set, infer it from the table
		if partition.Plugin == nil {
			partition.Plugin = plugin.NewPlugin(partition.InferPluginName(v))
		}
	}
}

// GetPluginForTable returns the plugin name that provides the given table.
// Falls back to first segment of name split on '_' if not found in metadata.
func (c *TailpipeConfig) GetPluginForTable(tableName string) string {
	// Check if the table is a custom table as these come from the `core` plugin
	if _, ok := c.CustomTables[tableName]; ok {
		return constants.CorePluginFullName
	}

	return GetPluginForTable(tableName, c.PluginVersions)
}

// GetPluginForTable : we need a separate function for this as we need to call it from the partition creation code,
// which is called before the TailpipeConfig is fully populated
func GetPluginForTable(tableName string, v map[string]*versionfile.InstalledVersion) string {
	// Check metadata tables for each plugin to determine a match
	for pluginName, version := range v {
		if tables, ok := version.Metadata["tables"]; ok {
			for _, table := range tables {
				if table == tableName {
					return pluginName
				}
			}
		}
	}
	// Fallback to first segment of name if no plugin found and not a custom table
	parts := strings.Split(tableName, "_")
	return parts[0]
}

// GetPluginForFormatPreset returns the plugin name that provides the given format [preset.
// Format name should be in the format "type.name"
func (c *TailpipeConfig) GetPluginForFormatPreset(fullName string) (string, bool) {
	// Check format_presets in metadata
	for pluginName, version := range c.PluginVersions {
		if presets, ok := version.Metadata["format_presets"]; ok {
			for _, preset := range presets {
				if preset == fullName {
					return pluginName, true
				}
			}
		}
	}

	return "", false
}

func (c *TailpipeConfig) GetPluginForFormatType(typeName string) (string, bool) {
	// Check format_types in metadata
	for pluginName, version := range c.PluginVersions {
		if types, ok := version.Metadata["format_types"]; ok {
			for _, t := range types {
				if t == typeName {
					return pluginName, true
				}
			}
		}
	}

	return "", false
}

// GetPluginForSourceType returns the plugin name that provides the given source.
func (c *TailpipeConfig) GetPluginForSourceType(sourceType string) string {
	// Check sources in metadata
	for pluginName, version := range c.PluginVersions {
		if sources, ok := version.Metadata["sources"]; ok {
			for _, source := range sources {
				if source == sourceType {
					return pluginName
				}
			}
		}
	}
	return strings.Split(sourceType, "_")[0]
}
