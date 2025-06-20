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
	// tactical - because up until sdk v0.8.0, the returned schema was not merged with the common schema,
	// we need to merge it here
	// otherwise the `required` property for common fields will not be set
	s := schema.TableSchemaFromProto(resp.Schema).MergeWithCommonSchema()

	return &CollectResponse{
		ExecutionId: resp.ExecutionId,
		Schema:      s,
		FromTime:    row_source.ResolvedFromTimeFromProto(resp.FromTime),
	}
}
