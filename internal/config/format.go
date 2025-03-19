package config

import (
	"fmt"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/v2/cty_helpers"
	"github.com/turbot/pipe-fittings/v2/hclhelpers"
	"github.com/turbot/pipe-fittings/v2/modconfig"
	"github.com/turbot/pipe-fittings/v2/schema"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/zclconf/go-cty/cty"
)

func init() {
	// we have a subtype - register it and ALSO implement GetSubType
	registerResourceWithSubType(schema.BlockTypeFormat)
}

type Format struct {
	modconfig.HclResourceImpl
	// the format type
	Type string `cty:"type"`
	// the raw HCL of the format (this will be decoded by the plugin which implements the format)
	Config *HclBytes `cty:"config"`
	// alternatively, the preset name opf the format
	Preset string `cty:"preset"`
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
			Summary:  "'format' block requires 2 labels: 'type' and 'name'",
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

func NewPresetFormat(block *hcl.Block, presetName string) (*Format, hcl.Diagnostics) {
	var diags hcl.Diagnostics
	parts := strings.Split(presetName, ".")
	if len(parts) != 2 {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "'format' block requires 2 labels: 'type' and 'name'",
			Subject:  hclhelpers.BlockRangePointer(block),
		})
		return nil, diags
	}

	return &Format{
		HclResourceImpl: modconfig.NewHclResourceImpl(&hcl.Block{}, presetName),
		Preset:          presetName,
		Type:            parts[0],
	}, diags
}

func (f *Format) ToProto() *proto.FormatData {
	res := &proto.FormatData{
		Name: f.ShortName,
	}
	// set either preset name or config
	if f.Preset != "" {
		res.Preset = f.Preset
	} else if f.Config != nil {
		res.Config = &proto.ConfigData{
			Target: "format." + f.Type,
			Hcl:    f.Config.Hcl,
			Range:  proto.RangeToProto(f.Config.Range.HclRange()),
		}
	}
	return res
}

// CtyValue implements CtyValueProvider
// (note this must be implemented by each resource, we cannot rely on the HclResourceImpl implementation as it will
// only serialise its own properties) )
func (f *Format) CtyValue() (cty.Value, error) {
	return cty_helpers.GetCtyValue(f)
}

func (f *Format) SetConfigHcl(u *HclBytes) {
	f.Config = u
}
