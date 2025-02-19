package config

import (
	"fmt"
	"strings"

	"github.com/hashicorp/hcl/v2"
	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/v2/hclhelpers"
	"github.com/turbot/pipe-fittings/v2/modconfig"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type Table struct {
	modconfig.HclResourceImpl

	// the default format for this table (todo make a map keyed by source name?)
	DefaultSourceFormat *Format `hcl:"format" cty:"format"`

	Columns []Column `hcl:"column,block" cty:"columns"`

	// should we include ALL source fields in addition to any defined columns, or ONLY include the columns defined
	AutoMapSourceFields bool `hcl:"automap_source_fields,optional" cty:"automap_source_fields"`
	// should we exclude any source fields from the output (only applicable if automap_source_fields is true)
	ExcludeSourceFields []string `hcl:"exclude_source_fields,optional" cty:"exclude_source_fields"`
	// the default null value for the table (may be overridden for specific columns)
	NullValue string `hcl:"null_value,optional" cty:"null_value"`
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
		AutoMapSourceFields: true,
	}

	// NOTE: as tailpipe does not have the concept of mods, the full name is table.<name> and
	// the unqualified name AND the short name is <name>
	c.UnqualifiedName = c.ShortName

	return c, nil
}

func (t *Table) ToProtoSchema() *proto.Schema {
	var res = &proto.Schema{
		AutomapSourceFields: t.AutoMapSourceFields,
		ExcludeSourceFields: t.ExcludeSourceFields,
		Description:         typehelpers.SafeString(t.Description),
		NullValue:           typehelpers.SafeString(t.NullValue),
	}
	for _, col := range t.Columns {
		s := &proto.ColumnSchema{
			// default source to column name
			SourceName:  col.Name,
			ColumnName:  col.Name,
			Type:        typehelpers.SafeString(col.Type),
			Description: typehelpers.SafeString(col.Description),
			NullValue:   typehelpers.SafeString(col.NullValue),
			Required:    typehelpers.BoolValue(col.Required),
		}
		// override the source name if it is set
		if col.Source != nil {
			s.SourceName = *col.Source
		}
		res.Columns = append(res.Columns, s)
	}
	return res
}

func (t *Table) ToProto() *proto.Table {
	return &proto.Table{
		Name:   t.ShortName,
		Schema: t.ToProtoSchema(),
	}
}

func (t *Table) Validate() hcl.Diagnostics {
	var diags hcl.Diagnostics
	// build list of optional columns without types
	var optionalColumnsWithNoType []string
	if !t.AutoMapSourceFields && len(t.ExcludeSourceFields) > 0 {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  fmt.Sprintf("Table '%s' failed validation", t.ShortName),
			Detail:   "exclude_source_fields can only be set if automap_source_fields is true",
			Subject:  t.DeclRange.Ptr(),
		})
	}

	for _, col := range t.Columns {
		// if the column is options, a type must be specified
		// - this is to ensure we can determine the column type in the case of the column being missing in the source data
		// (if the column is required, the column being missing would cause an error so this problem will not arise)

		// (tp columns are excluded from this as we know their types)
		if schema.IsCommonField(col.Name) {
			continue
		}

		if !typehelpers.BoolValue(col.Required) && col.Type == nil {
			optionalColumnsWithNoType = append(optionalColumnsWithNoType, col.Name)
		}
	}
	if len(optionalColumnsWithNoType) > 0 {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  fmt.Sprintf("Table '%s' failed validation", t.ShortName),
			Detail:   fmt.Sprintf("column type must be specified if column is optional (%s '%s')", utils.Pluralize("column", len(optionalColumnsWithNoType)), strings.Join(optionalColumnsWithNoType, "', '")),
			Subject:  t.DeclRange.Ptr(),
		})
	}
	return diags
}
