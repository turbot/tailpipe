package config

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/spf13/viper"
	"github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/hclhelpers"
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

func (c *TailpipeConfig) EqualConfig(want *TailpipeConfig) bool {
	if c == nil || want == nil {
		return c == want
	}
	// PluginVersions: presence, size, keys, then deep compare values (with presence guards)
	if (c.PluginVersions == nil) != (want.PluginVersions == nil) {
		return false
	}
	if c.PluginVersions != nil {
		if len(c.PluginVersions) != len(want.PluginVersions) {
			return false
		}
		for k, v := range c.PluginVersions {
			wv, ok := want.PluginVersions[k]
			if !ok {
				return false
			}
			if (v == nil) != (wv == nil) {
				return false
			}
			if v != nil {
				// Compare stable identity/structural fields; ignore LastCheckedDate, InstallDate
				if v.Name != wv.Name ||
					v.Version != wv.Version ||
					v.ImageDigest != wv.ImageDigest ||
					v.BinaryDigest != wv.BinaryDigest ||
					v.BinaryArchitecture != wv.BinaryArchitecture ||
					v.InstalledFrom != wv.InstalledFrom ||
					v.StructVersion != wv.StructVersion {
					return false
				}

				// Metadata map: presence, size, keys, then value slices order-insensitive
				if (v.Metadata == nil) != (wv.Metadata == nil) {
					return false
				}
				if v.Metadata != nil {
					if len(v.Metadata) != len(wv.Metadata) {
						return false
					}
					for mk, ma := range v.Metadata {
						mb, ok := wv.Metadata[mk]
						if !ok {
							return false
						}
						if len(ma) != len(mb) {
							return false
						}
						ac := make([]string, len(ma))
						bc := make([]string, len(mb))
						copy(ac, ma)
						copy(bc, mb)
						sort.Strings(ac)
						sort.Strings(bc)
						for i := range ac {
							if ac[i] != bc[i] {
								return false
							}
						}
					}
				}
			}
		}
	}

	// Partitions: presence, size, keys, then Partition.EqualConfig
	if (c.Partitions == nil) != (want.Partitions == nil) {
		return false
	}
	if len(c.Partitions) != len(want.Partitions) {
		return false
	}
	for k, p := range c.Partitions {
		wp, ok := want.Partitions[k]
		if !ok {
			return false
		}
		if !p.EqualConfig(wp) {
			return false
		}
	}

	// Connections: presence, size, keys, then deep compare fields with presence guards
	if (c.Connections == nil) != (want.Connections == nil) {
		return false
	}
	if c.Connections != nil {
		if len(c.Connections) != len(want.Connections) {
			return false
		}
		for k, conn := range c.Connections {
			wconn, ok := want.Connections[k]
			if !ok {
				return false
			}
			if (conn == nil) != (wconn == nil) {
				return false
			}
			if conn != nil {
				// identity metadata (ignore DeclRange for stability)
				if conn.HclResourceImpl.FullName != wconn.HclResourceImpl.FullName ||
					conn.HclResourceImpl.ShortName != wconn.HclResourceImpl.ShortName ||
					conn.HclResourceImpl.UnqualifiedName != wconn.HclResourceImpl.UnqualifiedName ||
					conn.HclResourceImpl.BlockType != wconn.HclResourceImpl.BlockType {
					return false
				}
				// plugin string
				if conn.Plugin != wconn.Plugin {
					return false
				}
				if conn.HclRange == (hclhelpers.Range{}) && wconn.HclRange == (hclhelpers.Range{}) {
					return false
				}
				if conn.HclRange != (hclhelpers.Range{}) && wconn.HclRange != (hclhelpers.Range{}) {
					if !reflect.DeepEqual(conn.HclRange, wconn.HclRange) {
						return false
					}
				}
			}
		}
	}

	// CustomTables: presence, size, keys, then deep compare key fields
	if (c.CustomTables == nil) != (want.CustomTables == nil) {
		return false
	}
	if c.CustomTables != nil {
		if len(c.CustomTables) != len(want.CustomTables) {
			return false
		}
		for k, ct := range c.CustomTables {
			wct, ok := want.CustomTables[k]
			if !ok {
				return false
			}
			if (ct == nil) != (wct == nil) {
				return false
			}
			if ct != nil {
				// identity metadata
				if ct.HclResourceImpl.FullName != wct.HclResourceImpl.FullName ||
					ct.HclResourceImpl.ShortName != wct.HclResourceImpl.ShortName ||
					ct.HclResourceImpl.UnqualifiedName != wct.HclResourceImpl.UnqualifiedName ||
					ct.HclResourceImpl.BlockType != wct.HclResourceImpl.BlockType {
					return false
				}
				// default format: only compare if both present
				if ct.DefaultSourceFormat != nil && wct.DefaultSourceFormat != nil {
					if ct.DefaultSourceFormat.Type != wct.DefaultSourceFormat.Type {
						return false
					}
					if ct.DefaultSourceFormat.PresetName != wct.DefaultSourceFormat.PresetName {
						return false
					}
					if ct.DefaultSourceFormat.HclResourceImpl.FullName != wct.DefaultSourceFormat.HclResourceImpl.FullName ||
						ct.DefaultSourceFormat.HclResourceImpl.ShortName != wct.DefaultSourceFormat.HclResourceImpl.ShortName ||
						ct.DefaultSourceFormat.HclResourceImpl.UnqualifiedName != wct.DefaultSourceFormat.HclResourceImpl.UnqualifiedName ||
						ct.DefaultSourceFormat.HclResourceImpl.BlockType != wct.DefaultSourceFormat.HclResourceImpl.BlockType {
						return false
					}
				}
				// columns: length and per-column key fields
				if len(ct.Columns) != len(wct.Columns) {
					return false
				}
				for i := range ct.Columns {
					ac := ct.Columns[i]
					bc := wct.Columns[i]
					if ac.Name != bc.Name {
						return false
					}
					// compare optional pointers only when both exist
					if ac.Type != nil && bc.Type != nil && *ac.Type != *bc.Type {
						return false
					}
					if ac.Source != nil && bc.Source != nil && *ac.Source != *bc.Source {
						return false
					}
					if ac.Description != nil && bc.Description != nil && *ac.Description != *bc.Description {
						return false
					}
					if ac.Required != nil && bc.Required != nil && *ac.Required != *bc.Required {
						return false
					}
					if ac.NullIf != nil && bc.NullIf != nil && *ac.NullIf != *bc.NullIf {
						return false
					}
					if ac.Transform != nil && bc.Transform != nil && *ac.Transform != *bc.Transform {
						return false
					}
				}
				// map_fields (order-insensitive). Treat empty as default ["*"].
				var mfA []string
				var mfB []string
				if len(ct.MapFields) == 0 {
					mfA = []string{"*"}
				} else {
					mfA = append([]string(nil), ct.MapFields...)
				}
				if len(wct.MapFields) == 0 {
					mfB = []string{"*"}
				} else {
					mfB = append([]string(nil), wct.MapFields...)
				}

				if len(mfA) != len(mfB) {
					return false
				}
				sort.Strings(mfA)
				sort.Strings(mfB)
				for i := range mfA {
					if mfA[i] != mfB[i] {
						return false
					}
				}
				// null_if
				if ct.NullIf != wct.NullIf {
					return false
				}
			}
		}
	}

	// Formats: presence, size, keys, then deep compare stable fields (ignore Config.Hcl bytes)
	if (c.Formats == nil) != (want.Formats == nil) {
		return false
	}
	if c.Formats != nil {
		if len(c.Formats) != len(want.Formats) {
			return false
		}
		for k, f := range c.Formats {
			wf, ok := want.Formats[k]
			if !ok {
				return false
			}
			if (f == nil) != (wf == nil) {
				return false
			}
			if f != nil {
				if f.Type != wf.Type {
					return false
				}
				if f.HclResourceImpl.FullName != wf.HclResourceImpl.FullName ||
					f.HclResourceImpl.ShortName != wf.HclResourceImpl.ShortName ||
					f.HclResourceImpl.UnqualifiedName != wf.HclResourceImpl.UnqualifiedName ||
					f.HclResourceImpl.BlockType != wf.HclResourceImpl.BlockType {
					return false
				}
				// preset name: compare only if both present; ok if only one side empty
				if f.PresetName != "" && wf.PresetName != "" && f.PresetName != wf.PresetName {
					return false
				}
				// Ignore config presence and range/Hcl entirely for stability
			}
		}
	}

	return true
}

// helpers removed; logic inlined in EqualConfig

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
		// Check if a partition with the same name already exists
		if _, exists := c.Partitions[t.GetUnqualifiedName()]; exists {
			return fmt.Errorf("partition %s already exists for table %s", t.ShortName, t.TableName)
		}
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
