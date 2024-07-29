package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

// TODO handle 3 part names
/*
Sources are where logs are collected from. They can be local files, remote
files, or APIs. This source is for an S3 bucket that includes logs.
Sources are agnostic to the logs that they contain, each source may by used
by multiple collectors.

source "aws_s3" "logs" {
	bucket = "my-bucket"
	prefix = "logs/" // optional, default is no prefix
	region = "us-west-2"
	credential = credential.aws.aws_org_root
}

Slack is a simpler source, really just defined through credentials since the
endpoint is well known.
TODO - Is this required? Or should collections accept either a source or a credential?

source "slack" "default" {
	credential = credential.slack.default
}
*/

type Source struct {
	modconfig.HclResourceImpl

	Type      string `hcl:"type,label"`
	ShortName string `hcl:"name,label"`

	// any collection specific config data for the collection
	Config []byte
}

func (s Source) ToProto() *proto.ConfigData {
	return &proto.ConfigData{
		Type:       s.Type,
		ConfigData: s.Config,
	}
}

func NewSource(block *hcl.Block, fullName string) (modconfig.HclResource, hcl.Diagnostics) {
	if len(block.Labels) < 2 {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "'source' block requires 2 labels, 'type' and 'name'",
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}

	return &Source{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fullName),
		Type:            block.Labels[0],
	}, nil
}
