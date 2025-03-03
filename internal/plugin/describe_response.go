package plugin

import (
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type PluginDescribeResponse struct {
	Name         string                       `json:"name"`
	TableSchemas schema.SchemaMap             `json:"tables"`
	Sources      row_source.SourceMetadataMap `json:"sources"`
	Formats	  formats.FormatDescriptionMap `json:"formats"`
}

func DescribeResponseFromProto(resp *proto.DescribeResponse) *PluginDescribeResponse {
	res := &PluginDescribeResponse{}
	if resp == nil {
		return res
	}
	if resp.Schemas != nil {
		res.TableSchemas = schema.SchemaMapFromProto(resp.Schemas)
	}
	if resp.Sources != nil {
		res.Sources = row_source.SourceMetadataMapFromProto(resp.Sources)
	}
	if resp.Formats != nil {
		res.Formats = formats.FormatMapFromProto(resp.Formats)
	}
	return res
}
