package config

import (
	"github.com/hashicorp/hcl/v2"
	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type ColumnSchema struct {
	Name   string  `hcl:"name,label" cty:"name"`
	Type   *string `hcl:"type" cty:"type"`
	Source *string `hcl:"source" cty:"source"`
}

type Table struct {
	modconfig.HclResourceImpl

	// the default format for this table (todo make a map keyed by source name?)
	DefaultSourceFormat *Format `hcl:"format" cty:"format"`

	Columns []ColumnSchema `hcl:"column,block" cty:"columns"`

	// should we include ALL source fields in addition to any defined columns, or ONLY include the columns defined
	AutoMapSourceFields bool `hcl:"automap_source_fields,optional" cty:"automap_source_fields"`
	// should we exclude any source fields from the output (only applicable if automap_source_fields is true)
	ExcludeSourceFields []string `hcl:"exclude_source_fields,optional" cty:"exclude_source_fields"`
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
		Name:   t.ShortName,
		Schema: t.ToProtoSchema(),
	}
}
