package plugin_manager

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type PluginDescribeResponse struct {
	Name         string                       `json:"name"`
	TableSchemas schema.SchemaMap             `json:"tables"`
	Sources      row_source.SourceMetadataMap `json:"sources"`
}

func DescribeResponseFromProto(resp *proto.DescribeResponse) *PluginDescribeResponse {
	return &PluginDescribeResponse{
		TableSchemas: schema.SchemaMapFromProto(resp.Schemas),
		Sources:      row_source.SourceMetadataMapFromProto(resp.Sources),
	}
}
