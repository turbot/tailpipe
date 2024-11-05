package config

import (
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/pipe-fittings/plugin"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"strings"
)

type Partition struct {
	modconfig.HclResourceImpl

	// Type of the partition
	Table string `hcl:"type,label"`

	// Plugin used for this partition
	Plugin *plugin.Plugin

	// Source of the data for this partition
	Source Source `hcl:"source,block"`

	// The connection
	Connection *TailpipeConnection `hcl:"connection"`

	// any partition-type specific config data for the partition
	Config []byte
	// the config location
	ConfigRange hcl.Range
}

func NewPartition(block *hcl.Block, fullName string) (modconfig.HclResource, hcl.Diagnostics) {
	if len(block.Labels) < 2 {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "'partition' block requires 2 labels, 'type' and 'name'",
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}
	c := &Partition{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fullName),
		Table:           block.Labels[0],
	}

	// NOTE: as tailpipe does not have the concept of mods, the full name is partition.<type>.<name> and
	// the unqualified name is the <type>.<name>
	c.UnqualifiedName = fmt.Sprintf("%s.%s", c.Table, c.ShortName)
	return c, nil
}

func (c *Partition) OnDecoded(block *hcl.Block, _ modconfig.ModResourcesProvider) hcl.Diagnostics {
	// if plugin is not set, deduce it from the type
	if c.Plugin == nil {
		name := c.inferPluginName()
		c.Plugin = &plugin.Plugin{
			Instance: name,
			Alias:    name,
			Plugin:   plugin.ResolvePluginImageRef(name),
		}
	}
	// TODO K default connections
	// if the connection is not set, set it to the default connection
	//if c.Connection == nil {
	//	c.Connection = getDefaultConnection(c.Plugin.Alias)
	//}
	return nil
}

func getDefaultConnection(alias string) *TailpipeConnection {
	// TODO: think about default connections
	return &TailpipeConnection{
		Plugin: alias,
	}
}

func (c *Partition) ToProto() *proto.ConfigData {
	return &proto.ConfigData{
		Type:  c.Table,
		Hcl:   c.Config,
		Range: proto.RangeToProto(c.DeclRange),
	}
}

func (c *Partition) SetConfigHcl(u *HclBytes) {
	if u == nil {
		return
	}
	c.Config = u.Hcl
	c.ConfigRange = u.Range
}

func (c *Partition) inferPluginName() string {
	return strings.Split(c.Table, "_")[0]
}
