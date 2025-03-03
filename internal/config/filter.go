package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/v2/cty_helpers"
	"github.com/turbot/pipe-fittings/v2/modconfig"
	"github.com/zclconf/go-cty/cty"
)

type Filter struct {
	modconfig.HclResourceImpl
	Where *string `hcl:"where"`
}

func NewFilter(block *hcl.Block, fullName string) (modconfig.HclResource, hcl.Diagnostics) {
	return &Filter{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fullName),
	}, nil
}

// CtyValue implements CtyValueProvider
// (note this must be implemented by each resource, we cannot rely on the HclResourceImpl implementation as it will
// only serialise its own properties) )
func (f *Filter) CtyValue() (cty.Value, error) {
	return cty_helpers.GetCtyValue(f)
}
