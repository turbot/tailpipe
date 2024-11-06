package config

import (
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type TailpipeConnection struct {
	modconfig.HclResourceImpl
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
		//Target:  c.Name(),
		// TODO fix connection parsing to populate name
		Target: "connection." + c.Plugin,

		Hcl:   c.Hcl,
		Range: proto.RangeToProto(c.DeclRange),
	}
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

// TODO K implement if needed
//
//func CtyValueToConnection(value cty.Value) (_ *TailpipeConnection, err error) {
//	defer func() {
//		if r := recover(); r != nil {
//			err = perr.BadRequestWithMessage("unable to decode connection: " + r.(string))
//		}
//	}()
//
//	// get the name, block type and range and use to construct a connection
//	shortName := value.GetAttr("short_name").AsString()
//	name := value.GetAttr("name").AsString()
//	block := &hcl.Block{
//		Labels: []string{"connection", name},
//	}
//
//
//
//	// now instantiate an empty connection of the correct type
//	conn, err := NewTailpipeConnection(&hcl.Block{}, name)
//	if err != nil {
//		return nil, perr.BadRequestWithMessage("unable to decode connection: " + err.Error())
//	}
//
//	// split the cty value into fields for ConnectionImpl and the derived connection,
//	// (NOTE: exclude the 'env', 'type', 'resource_type' fields, which are manually added)
//	baseValue, derivedValue, err := getKnownCtyFields(value, conn.GetConnectionImpl(), "env", "type", "resource_type")
//	if err != nil {
//		return nil, perr.BadRequestWithMessage("unable to decode connection: " + err.Error())
//	}
//	// decode the base fields into the ConnectionImpl
//	err = gocty.FromCtyValue(baseValue, conn.GetConnectionImpl())
//	if err != nil {
//		return nil, perr.BadRequestWithMessage("unable to decode ConnectionImpl: " + err.Error())
//	}
//	// decode remaining fields into the derived connection
//	err = gocty.FromCtyValue(derivedValue, &conn)
//	if err != nil {
//		return nil, perr.BadRequestWithMessage("unable to decode connection: " + err.Error())
//	}
//
//	return nil, nil
//}
