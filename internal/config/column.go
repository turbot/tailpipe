package config

import (
	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Column struct {
	// The name of the column. This is the name that will be used in the output.
	Name string `hcl:"name,label" cty:"name"`
	// The DuckDB name of the column in the source.
	Type *string `hcl:"type" cty:"type"`
	// The source name of the column. This is the field name in the source data
	Source *string `hcl:"source" cty:"source"`
	// An optional description of the column.
	Description *string `hcl:"description" cty:"description"`
	// Is the column required?
	Required *bool `hcl:"required" cty:"required"`
	// An optional null value for the column. If the source value contains this, the column will be null
	NullIf *string `hcl:"null_if" cty:"null_if"`
	// A duck DB transform function to apply to the column. This should be expressed as a SQL function
	// If a Transform is provided, no source should be provided.
	// e.g. "upper(name)"
	Transform *string `hcl:"transform" cty:"transform"`
}

func (c Column) ToProto() *proto.ColumnSchema {
	s := &proto.ColumnSchema{
		// default source to column name
		SourceName:  c.Name,
		ColumnName:  c.Name,
		Type:        typehelpers.SafeString(c.Type),
		Description: typehelpers.SafeString(c.Description),
		NullValue:   typehelpers.SafeString(c.NullIf),
		Required:    typehelpers.BoolValue(c.Required),
		Transform:   typehelpers.SafeString(c.Transform),
	}
	// override the source name if it is set
	if c.Source != nil {
		s.SourceName = *c.Source
	}
	return s
}
