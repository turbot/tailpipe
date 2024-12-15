package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
)

type Table struct {
	modconfig.HclResourceImpl

	Base string `hcl:"base"`
}

func NewTable(block *hcl.Block, fullName string) (modconfig.HclResource, hcl.Diagnostics) {
	if len(block.Labels) < 2 {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "'tableName' block requires 2 labels, 'type' and 'name'",
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}
	c := &Table{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fullName),
	}

	// NOTE: as tailpipe does not have the concept of mods, the full name is table.<name> and
	// the unqualified name AND the short name is <name>
	c.UnqualifiedName = c.ShortName
	return c, nil
}

func (c *Table) OnDecoded(block *hcl.Block, _ modconfig.ModResourcesProvider) hcl.Diagnostics {
	return nil
}
