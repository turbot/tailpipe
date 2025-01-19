package config

import (
	"fmt"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/pipe-fittings/plugin"
	"github.com/turbot/pipe-fittings/versionfile"
)

type TailpipeConfig struct {
	// map of partitions, keyed by unqualified name (<partition_type>.<partition_name>)
	Partitions map[string]*Partition

	Connections map[string]*TailpipeConnection

	// map of plugin configs, keyed by plugin image ref
	// (for each image ref we store an array of configs)
	//Plugins map[string][]*plugin.Plugin
	//// map of plugin configs, keyed by plugin instance
	//PluginsInstances map[string]plugin.Plugin
	// map of installed plugin versions, keyed by plugin image ref
	PluginVersions map[string]*versionfile.InstalledVersion
	CustomTables   map[string]*Table
	Formats        map[string]*Format
}

func NewTailpipeConfig() *TailpipeConfig {
	return &TailpipeConfig{
		Partitions:  make(map[string]*Partition),
		Connections: make(map[string]*TailpipeConnection),
		//Plugins:     make(map[string][]*plugin.Plugin),
		//PluginsInstances: make(map[string]plugin.Plugin),
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

func (c *TailpipeConfig) Validate() error {
	// TODO K
	return nil
}

func (c *TailpipeConfig) InitPartitions() {
	// populate the plugin property for each partition
	for _, partition := range c.Partitions {
		// set the table on the plugin (in case it is a custom table)
		if _, isCustom := c.CustomTables[partition.TableName]; isCustom {
			partition.CustomTable = c.CustomTables[partition.TableName]
		}
		// if the plugin is not set, infer it from the table
		if partition.Plugin == nil {
			partition.Plugin = plugin.NewPlugin(partition.InferPluginName())
		}
	}
}
