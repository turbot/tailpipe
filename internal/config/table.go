package config

import (
	"github.com/hashicorp/hcl/v2"
	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type ColumnSchema struct {
	Name   string  `hcl:"name,label" cty:"name"`
	Type   *string `hcl:"type" cty:"type"`
	Source *string `hcl:"source" cty:"source"`
}

type Table struct {
	modconfig.HclResourceImpl

	// the default format for this table (todo make a map keyed by source name?)
	DefaultFormat *Format `hcl:"format" cty:"format"`

	Columns []ColumnSchema `hcl:"column,block" cty:"columns"`

	// one of :
	// "full"    - all columns specified - use this to exclude columns
	// "dynamic" - no columns specified - all will be inferred (this value will never be set as it's implicit if no columns are specified)
	// "partial" -the default - some columns specified explicitly, the rest will be inferred
	//
	Mode schema.Mode `hcl:"mode,optional"`
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
	}

	// NOTE: as tailpipe does not have the concept of mods, the full name is table.<name> and
	// the unqualified name AND the short name is <name>
	c.UnqualifiedName = c.ShortName
	// default the schem mode to partial
	c.Mode = schema.ModePartial
	return c, nil
}

func (t *Table) OnDecoded(block *hcl.Block, _ modconfig.ModResourcesProvider) hcl.Diagnostics {
	// validate the schema mode
	switch t.Mode {
	case schema.ModeFull, schema.ModePartial:
	// ok
	case schema.ModeDynamic:
		if len(t.Columns) > 0 {
			return hcl.Diagnostics{&hcl.Diagnostic{
				Severity: hcl.DiagError,
				Summary:  "table with mode 'dynamic' cannot have any columns specified",
				Subject:  hclhelpers.BlockRangePointer(block),
			}}
		}
	default:
		return hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "table mode must be one of 'full', 'partial' or 'dynamic'",
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}

	return nil
}

func (t *Table) ToProtoSchema() *proto.Schema {
	var res = &proto.Schema{
		Mode: string(t.Mode),
	}
	for _, col := range t.Columns {
		res.Columns = append(res.Columns, &proto.ColumnSchema{
			// source name and column name are the same in this case
			SourceName: col.Name,
			ColumnName: col.Name,
			Type:       typehelpers.SafeString(col.Type),
		})
	}
	return res
}

func (t *Table) ToProto() *proto.Table {
	return &proto.Table{
		Name:         t.ShortName,
		SourceFormat: t.DefaultFormat.ToProto(),
		Schema:       t.ToProtoSchema(),
	}
}
