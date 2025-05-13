package config

import (
	"fmt"
	"strings"

	"github.com/hashicorp/hcl/v2"
	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/v2/cty_helpers"
	"github.com/turbot/pipe-fittings/v2/hclhelpers"
	"github.com/turbot/pipe-fittings/v2/modconfig"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/zclconf/go-cty/cty"
)

// Table is a struct representing a custom table definition
type Table struct {
	modconfig.HclResourceImpl
	// required to allow partial decoding
	Remain hcl.Body `hcl:",remain" json:"-"`

	// the default format for this table (todo make a map keyed by source name?)
	DefaultSourceFormat *Format `hcl:"format" cty:"format"`

	Columns []Column `hcl:"column,block" cty:"columns"`

	// should we include ALL source fields in addition to any defined columns, or ONLY include the columns defined
	// default to automap ALL source fields (*)
	MapFields []string `hcl:"map_fields,optional" cty:"map_fields"`
	// the default null value for the table (may be overridden for specific columns)
	NullIf string `hcl:"null_if,optional" cty:"null_if"`
}

func NewTable(block *hcl.Block, fullName string) (modconfig.HclResource, hcl.Diagnostics) {
	if len(block.Labels) != 1 {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "'table' block requires 1 label, 'name'",
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}
	c := &Table{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fullName),
		// default to automap source fields
		MapFields: []string{"*"},
	}

	// NOTE: as tailpipe does not have the concept of mods, the full name is table.<name> and
	// the unqualified name AND the short name is <name>
	c.UnqualifiedName = c.ShortName

	return c, nil
}

func (t *Table) ToProto() *proto.Schema {
	var res = &proto.Schema{
		Description: typehelpers.SafeString(t.Description),
		NullValue:   typehelpers.SafeString(t.NullIf),
		Name:        t.ShortName,
		MapFields:   t.MapFields,
	}
	for _, col := range t.Columns {
		res.Columns = append(res.Columns, col.ToProto())
	}
	return res
}

// CtyValue implements CtyValueProvider
// (note this must be implemented by each resource, we cannot rely on the HclResourceImpl implementation as it will
// only serialise its own properties) )
func (t *Table) CtyValue() (cty.Value, error) {
	return cty_helpers.GetCtyValue(t)
}

func (t *Table) Validate() hcl.Diagnostics {
	var diags hcl.Diagnostics
	// build list of optional columns without types
	var validationErrors []string

	for _, col := range t.Columns {
		// if the column is options, a type must be specified
		// - this is to ensure we can determine the column type in the case of the column being missing in the source data
		// (if the column is required, the column being missing would cause an error so this problem will not arise)

		// (tp columns are excluded from this as we know their types)
		if schema.IsCommonField(col.Name) {
			continue
		}

		// check the type is valid
		if col.Source != nil && col.Transform != nil {
			validationErrors = append(validationErrors, fmt.Sprintf("column '%s': source and transform cannot both be set", col.Name))
		}

		// if there is a transform, a type is not required (as the output of the transform will determine the type)
		if col.Transform != nil {
			continue
		}
		if !typehelpers.BoolValue(col.Required) && col.Type == nil {
			validationErrors = append(validationErrors, fmt.Sprintf("column '%s': type must be specified if column is optional ", col.Name))
		}

		if col.Type != nil && !schema.IsValidColumnType(typehelpers.SafeString(col.Type)) {
			validationErrors = append(validationErrors, fmt.Sprintf("column '%s': type '%s' is not a valid type", col.Name, typehelpers.SafeString(col.Type)))
		}
	}
	if len(validationErrors) > 0 {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  fmt.Sprintf("Table '%s' failed validation", t.ShortName),
			Detail:   strings.Join(validationErrors, "\n"),
			Subject:  t.DeclRange.Ptr(),
		})
	}
	return diags
}
