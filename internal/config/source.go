package config

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Source struct {
	Type       string              `hcl:" type,label"`
	Connection *TailpipeConnection `hcl:"connection"`
	// the config hcl
	Config *HclBytes
}

func (s *Source) ToProto() *proto.ConfigData {
	return &proto.ConfigData{
		Target: "source." + s.Type,
		Hcl:    s.Config.Hcl,
		Range:  proto.RangeToProto(s.Config.Range),
	}
}

func (s *Source) SetConfigHcl(u *HclBytes) {
	s.Config = u
}
