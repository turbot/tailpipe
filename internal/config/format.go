package config

import (
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/v2/hclhelpers"
	"github.com/turbot/pipe-fittings/v2/modconfig"
	"github.com/turbot/pipe-fittings/v2/schema"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

func init() {
	// we have a subtype - register it and ALSO implement GetSubType
	registerResourceWithSubType(schema.BlockTypeFormat)
}

type Format struct {
	modconfig.HclResourceImpl

	Type   string    `cty:"type"`
	Config *HclBytes `cty:"config"`
}

// GetSubType returns the subtype for the format block (the type).
// The presence of this function indicates this resource supports 3 part names,
// which affects how it is stored in the eval context
func (f *Format) GetSubType() string {
	return f.Type
}

func NewFormat(block *hcl.Block, fullName string) (modconfig.HclResource, hcl.Diagnostics) {
	if len(block.Labels) != 2 {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "'format' block requires 1 labels: 'type' and 'name'",
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}
	c := &Format{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fullName),
		Type:            block.Labels[0],
	}

	// NOTE: as tailpipe does not have the concept of mods, the full name is format.<type>.<name> and
	// the unqualified name is <type>.<name>
	c.UnqualifiedName = fmt.Sprintf("%s.%s", c.Type, c.ShortName)
	return c, nil
}

func (f *Format) ToProto() *proto.ConfigData {
	res := &proto.ConfigData{
		Target: "format." + f.Type,
	}
	if f.Config != nil {
		res.Hcl = f.Config.Hcl
		res.Range = proto.RangeToProto(f.Config.Range.HclRange())
	}
	return res
}

func (f *Format) SetConfigHcl(u *HclBytes) {
	f.Config = u
}
