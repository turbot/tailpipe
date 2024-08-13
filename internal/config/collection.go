package config

import (
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Collection struct {
	modconfig.HclResourceImpl

	// Type of the collection
	Type string `hcl:"type,label"`

	// Plugin used for this collection
	Plugin string `hcl:"plugin"`

	// Source of the data for this collection
	Source Source `hcl:"source,block"`

	// any collection specific config data for the collection
	Config []byte
}

func NewCollection(block *hcl.Block, fullName string) (modconfig.HclResource, hcl.Diagnostics) {
	if len(block.Labels) < 2 {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "'collection' block requires 2 labels, 'type' and 'name'",
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}
	c := &Collection{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fullName),
		Type:            block.Labels[0],
	}

	// NOTE: as tailpipe does not have the concept of mods, the full name is collection.<type>.<name> and
	// the unqualified name is the <type>.<name>
	c.UnqualifiedName = fmt.Sprintf("%s.%s", c.Type, c.ShortName)
	return c, nil
}

func (c *Collection) ToProto() *proto.ConfigData {
	return &proto.ConfigData{
		Type:       c.Type,
		ConfigData: c.Config,
	}
}

func (c *Collection) SetUnknownHcl(unknown map[*hclsyntax.Block][]byte) hcl.Diagnostics {
	var diags hcl.Diagnostics
	for k, v := range unknown {
		if k == nil {
			c.Config = v
			continue
		}

		if k.Type == "source" {
			c.Source.Config = v
			continue
		}

		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "failed to set unknown hcl",
			Detail:   fmt.Sprintf("unknown hcl for unsupported block: %s", k),
		})
	}
	return diags
}
