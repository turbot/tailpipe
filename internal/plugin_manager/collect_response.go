package plugin_manager

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type CollectResponse struct {
	ExecutionId     string
	PartitionSchema *schema.RowSchema
}

func CollectResponseFromProto(resp *proto.CollectResponse) *CollectResponse {
	return &CollectResponse{
		ExecutionId:     resp.ExecutionId,
		PartitionSchema: schema.RowSchemaFromProto(resp.Schema),
	}
}
