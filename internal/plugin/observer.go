package plugin

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Observer interface {
	Notify(event *proto.Event)
}
