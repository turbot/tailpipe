package config

import (
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Partition struct {
	modconfig.HclResourceImpl

	// Type of the partition
	Type string `hcl:"type,label"`

	// Plugin used for this partition
	Plugin string `hcl:"plugin"`

	// Source of the data for this partition
	Source Source `hcl:"source,block"`

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
		Type:            block.Labels[0],
	}

	// NOTE: as tailpipe does not have the concept of mods, the full name is partition.<type>.<name> and
	// the unqualified name is the <type>.<name>
	c.UnqualifiedName = fmt.Sprintf("%s.%s", c.Type, c.ShortName)
	return c, nil
}

func (c *Partition) ToProto() *proto.ConfigData {
	return &proto.ConfigData{
		Type:  c.Type,
		Hcl:   c.Config,
		Range: proto.RangeToProto(c.DeclRange),
	}
}

func (c *Partition) SetUnknownHcl(u *UnknownHcl) {
	if u == nil {
		return
	}
	c.Config = u.Hcl
	c.ConfigRange = u.Range
}
