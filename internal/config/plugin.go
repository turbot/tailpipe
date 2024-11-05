package config

import (
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
)

// TODO UNUSED AS YET - can we just use plugin.Plugin??? sort parsing
type TailpipePlugin struct {
	modconfig.HclResourceImpl
	Plugin  string `cty:"plugin"`
	Version string `cty:"version"`
}

// GetSubType returns the subtype for the plug (the plugin).
// The presence of this function indicates this resource supports 3 part names,
// which affects how it is stored in the eval context
func (c *TailpipePlugin) GetSubType() string {
	return c.Plugin
}

func NewTailpipePlugin(block *hcl.Block, fullName string) (modconfig.HclResource, hcl.Diagnostics) {
	if len(block.Labels) < 2 {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "'TailpipePlugin' block requires 2 labels, 'type' and 'name'",
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}
	c := &TailpipePlugin{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fullName),
		Plugin:          block.Labels[0],
	}

	// NOTE: as tailpipe does not have the concept of mods, the full name is plugin.<type>.<name> and
	// the unqualified name is the <type>.<name>
	c.UnqualifiedName = fmt.Sprintf("%s.%s", c.Plugin, c.ShortName)
	return c, nil
}
