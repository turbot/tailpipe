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
	Plugins map[string][]*plugin.Plugin
	// map of plugin configs, keyed by plugin instance
	PluginsInstances map[string]plugin.Plugin
	// map of installed plugin versions, keyed by plugin image ref
	PluginVersions map[string]*versionfile.InstalledVersion
}

func NewTailpipeConfig() *TailpipeConfig {
	return &TailpipeConfig{
		Partitions:  make(map[string]*Partition),
		Connections: make(map[string]*TailpipeConnection),
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
	default:
		return fmt.Errorf("unsupported resource type %T", t)
	}
}

func (c *TailpipeConfig) Validate() (warnins, errors []string) {
	// TODO: implement
	return nil, nil
}
