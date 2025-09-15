package config

import (
	"fmt"

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
	registerResourceWithSubType(schema.BlockTypeConnection)
}

type TailpipeConnection struct {
	modconfig.HclResourceImpl
	// required to allow partial decoding
	Remain hcl.Body `hcl:",remain" json:"-"`

	Plugin string `cty:"plugin"`
	Hcl    []byte `cty:"hcl"`
	// the hcl range for the connection - use our version so we can sty serialise it
	HclRange hclhelpers.Range `cty:"hcl_range"`
}

// GetSubType returns the subtype for the connection (the plugin).
// The presence of this function indicates this resource supports 3 part names,
// which affects how it is stored in the eval context
func (c *TailpipeConnection) GetSubType() string {
	return c.Plugin
}

func (c *TailpipeConnection) ToProto() *proto.ConfigData {
	return &proto.ConfigData{
		// Target is of form `connection.<type>`
		Target: "connection." + c.Plugin,
		Hcl:    c.Hcl,
		Range:  proto.RangeToProto(c.DeclRange),
	}
}

// CtyValue implements CtyValueProvider
// (note this must be implemented by each resource, we cannot rely on the HclResourceImpl implementation as it will
// only serialise its own properties) )
func (c *TailpipeConnection) CtyValue() (cty.Value, error) {
	return cty_helpers.GetCtyValue(c)
}

func NewTailpipeConnection(block *hcl.Block, fullName string) (modconfig.HclResource, hcl.Diagnostics) {
	if len(block.Labels) < 2 {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "'TailpipeConnection' block requires 2 labels, 'type' and 'name'",
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}
	c := &TailpipeConnection{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fullName),
		Plugin:          block.Labels[0],
	}

	// NOTE: as tailpipe does not have the concept of mods, the full name is connection.<type>.<name> and
	// the unqualified name is the <type>.<name>
	c.UnqualifiedName = fmt.Sprintf("%s.%s", c.Plugin, c.ShortName)
	return c, nil
}
