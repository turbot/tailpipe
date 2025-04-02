package plugin

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe/internal/config"
)

type DescribeOpts func(*proto.DescribeRequest)

func WithCustomFormats(customFormats ...*config.Format) DescribeOpts {
	return func(req *proto.DescribeRequest) {
		var customFormatsProto []*proto.FormatData
		for _, f := range customFormats {
			customFormatsProto = append(customFormatsProto, f.ToProto())
		}
		req.CustomFormats = customFormatsProto
	}
}

func WithCustomFormatsOnly() DescribeOpts {
	return func(req *proto.DescribeRequest) {
		req.CustomFormatsOnly = true
	}
}
