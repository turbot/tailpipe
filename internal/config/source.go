package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Source struct {
	Type   string `hcl:" type,label"`
	Config []byte
	// the config location
	ConfigRange hcl.Range
}

func (s *Source) ToProto() *proto.ConfigData {
	return &proto.ConfigData{
		Type:  s.Type,
		Hcl:   s.Config,
		Range: proto.RangeToProto(s.ConfigRange),
	}
}

func (s *Source) SetUnknownHcl(u *UnknownHcl) {
	if u == nil {
		return
	}
	s.Config = u.Hcl
	s.ConfigRange = u.Range
}
