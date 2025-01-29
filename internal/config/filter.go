package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/v2/modconfig"
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
