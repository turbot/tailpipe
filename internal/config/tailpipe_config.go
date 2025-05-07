package config

import (
	"fmt"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/spf13/viper"
	"github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/modconfig"
	"github.com/turbot/pipe-fittings/v2/plugin"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/pipe-fittings/v2/versionfile"
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

func (c *TailpipeConfig) InitPartitions(versionMap *versionfile.PluginVersionFile) {
	// populate the plugin property for each partition
	for _, partition := range c.Partitions {
		// set the table on the plugin (in case it is a custom table)
		if _, isCustom := c.CustomTables[partition.TableName]; isCustom {
			partition.CustomTable = c.CustomTables[partition.TableName]
		}
		// if the plugin is not set, infer it from the table
		if partition.Plugin == nil {
			partition.Plugin = plugin.NewPlugin(partition.InferPluginName(versionMap))
			// set memory limit on plugin struct
			if viper.IsSet(constants.ArgMemoryMaxMbPlugin) {
				partition.Plugin.MemoryMaxMb = utils.ToPointer(viper.GetInt(constants.ArgMemoryMaxMbPlugin))
			}
		}
	}
}

// GetPluginForTable returns the plugin name that provides the given table.
// NOTE: this does not check custom tables - if the same table name is a custom table we should use the core plugin
// we cannot check that here as this function may be called before the config is fully populated
func GetPluginForTable(tableName string, versionMap map[string]*versionfile.InstalledVersion) string {
	// Check metadata tables for each plugin to determine a match
	for pluginName, version := range versionMap {
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

// GetPluginForFormat determines the plugin which provides the given format, using the plugin versions file,
// Call either GetPluginForFormatPreset or GetPluginForFormatType
func GetPluginForFormat(format *Format) (string, bool) {
	// first check if this is a preset of a type of any installed plugin
	// this handles the case where a plugin has a preset in a format which is provided by cores
	var pluginName string
	var ok bool

	// presets formats are always provided by the plugin which defines the preset,
	// even if the type is provided by the core plugin
	if format.PresetName != "" {
		pluginName, ok = GetPluginForFormatPreset(format.PresetName, GlobalConfig.PluginVersions)
	} else {
		// otherwise just return the plugin which provides the type
		pluginName, ok = GetPluginForFormatType(format.Type, GlobalConfig.PluginVersions)
	}

	return pluginName, ok
}

// GetPluginForFormatByName returns the plugin name that provides the given format name.
// Call either GetPluginForFormatPreset or GetPluginForFormatType
func GetPluginForFormatByName(formatName string) (string, bool) {
	// is this a format defined in config? If so retrieve the format and call GetPluginForFormat
	format, ok := GlobalConfig.Formats[formatName]
	if ok {
		return GetPluginForFormat(format)
	}
	// otherwise is must be a preset
	return GetPluginForFormatPreset(formatName, GlobalConfig.PluginVersions)
}

// GetPluginForFormatPreset returns the plugin name that provides the given format preset.
// Format name should be in the format "type.name"
func GetPluginForFormatPreset(fullName string, versionMap map[string]*versionfile.InstalledVersion) (string, bool) {
	// remove the prefix as the presets registered int he version file do not have them
	fullName = strings.TrimPrefix(fullName, "format.")
	// Check format_presets in metadata
	for pluginName, version := range versionMap {
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

// GetPluginForFormatType returns the plugin name that provides the given format type.
func GetPluginForFormatType(typeName string, versionMap map[string]*versionfile.InstalledVersion) (string, bool) {
	// Check format_types in metadata
	for pluginName, version := range versionMap {
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
func GetPluginForSourceType(sourceType string, versionMap map[string]*versionfile.InstalledVersion) string {
	// Check sources in metadata
	for pluginName, version := range versionMap {
		if sources, ok := version.Metadata["sources"]; ok {
			for _, source := range sources {
				if source == sourceType {
					return pluginName
				}
			}
		}
	}

	// to support older plugin versions which have not registered their resources, fallback to the first segment of the name
	return strings.Split(sourceType, "_")[0]
}
