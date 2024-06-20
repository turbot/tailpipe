package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
)

/*
Credentials are used to store secret information only, similar to flowpipe.
credential "aws" "aws_org_root" {
    role = "..."
}
*/
// TODO do we need different credential types? what does Flowpipw do
// placeholder for now
type Credential struct {
	modconfig.HclResourceImpl

	Type string
	// TODO temp - need different types
	Role string `cty:"role"`
}

func NewCredential(block *hcl.Block, fullName string) (modconfig.HclResource, hcl.Diagnostics) {
	if len(block.Labels) < 2 {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "'credential' block requires 2 labels, 'type' and 'name'",
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}
	return &Credential{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fullName),
		Type:            block.Labels[0],
	}, nil
}
