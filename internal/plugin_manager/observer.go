package plugin_manager

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Observer interface {
	Notify(ctx context.Context, event *proto.Event)
}
