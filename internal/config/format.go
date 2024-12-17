package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Format struct {
	modconfig.HclResourceImpl

	Type   string    `cty:"type"`
	Config *HclBytes `cty:"config"`
}

func NewFormat(block *hcl.Block, fullName string) (modconfig.HclResource, hcl.Diagnostics) {
	if len(block.Labels) != 1 {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "'format' block requires 1 label: 'name'",
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}
	c := &Format{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fullName),
	}

	// NOTE: as tailpipe does not have the concept of mods, the full name is format.<type>.<name> and
	// the unqualified name is the short name
	c.UnqualifiedName = c.ShortName
	return c, nil
}

func (s *Format) ToProto() *proto.ConfigData {
	res := &proto.ConfigData{
		Target: s.Type,
	}
	if s.Config != nil {
		res.Hcl = s.Config.Hcl
		res.Range = proto.RangeToProto(s.Config.Range.HclRange())
	}
	return res
}

func (s *Format) SetConfigHcl(u *HclBytes) {
	s.Config = u
}
