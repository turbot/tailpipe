package config

import (
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/pipe-fittings/plugin"
)

type Format struct {
	modconfig.HclResourceImpl

	Base string `hcl:"base"`

	// Plugin used for this Format
	Plugin *plugin.Plugin
}

func NewFormat(block *hcl.Block, fullName string) (modconfig.HclResource, hcl.Diagnostics) {
	if len(block.Labels) < 2 {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "'Format' block requires 2 labels, 'type' and 'name'",
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}
	c := &Format{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fullName),
	}

	// NOTE: as tailpipe does not have the concept of mods, the full name is format.<name> and
	// the unqualified name AND the short name is <name>
	c.UnqualifiedName = c.ShortName
	return c, nil
}

func (c *Format) OnDecoded(block *hcl.Block, _ modconfig.ModResourcesProvider) hcl.Diagnostics {
	// if plugin is not set, deduce it from the type
	if c.Plugin == nil {
		c.Plugin = plugin.NewPlugin(c.inferPluginName())
	}
	return nil
}

func (c *Format) inferPluginName() string {
	// infer plugin from the base format
	return strings.Split(c.Base, "_")[0]
}
