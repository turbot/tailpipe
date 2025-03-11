package plugin

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type CollectResponse struct {
	ExecutionId string
	Schema      *schema.TableSchema
	FromTime    *row_source.ResolvedFromTime
}

func CollectResponseFromProto(resp *proto.CollectResponse) *CollectResponse {
	return &CollectResponse{
		ExecutionId: resp.ExecutionId,
		Schema:      schema.TableSchemaFromProto(resp.Schema),
		FromTime:    row_source.ResolvedFromTimeFromProto(resp.FromTime),
	}
}
