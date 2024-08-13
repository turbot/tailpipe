package config

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Source struct {
	Type   string `hcl:"type,label"`
	Config []byte
}

func (s *Source) ToProto() *proto.ConfigData {
	return &proto.ConfigData{
		Type:       s.Type,
		ConfigData: s.Config,
	}
}

// TODO only needed if source blocks are top level
//
//func (s *Source) SetUnknownHcl(unknown map[string][]byte) {
//	s.Config = unknown
//}
