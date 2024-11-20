package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Source struct {
	Type       string `hcl:" type,label"`
	Config     []byte
	Connection *TailpipeConnection `hcl:"connection"`

	// the config location
	ConfigRange hcl.Range
}

func (s *Source) ToProto() *proto.ConfigData {
	return &proto.ConfigData{
		Target: "source." + s.Type,
		Hcl:    s.Config,
		Range:  proto.RangeToProto(s.ConfigRange),
	}
}

func (s *Source) SetConfigHcl(u *HclBytes) {
	if u == nil {
		return
	}
	s.Config = u.Hcl
	s.ConfigRange = u.Range
}
