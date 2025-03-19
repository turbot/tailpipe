package config

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Source struct {
	Type       string              `hcl:" type,label" cty:"type"`
	Connection *TailpipeConnection `hcl:"connection" cty:"connection"`
	// optional: the format (for custom tables)
	Format *Format `hcl:"format" cty:"format"`
	// the config hcl
	Config *HclBytes `cty:"config"`
}

func (s *Source) ToProto() *proto.ConfigData {
	return &proto.ConfigData{
		Target: "source." + s.Type,
		Hcl:    s.Config.Hcl,
		Range:  proto.RangeToProto(s.Config.Range.HclRange()),
	}
}

func (s *Source) SetConfigHcl(u *HclBytes) {
	s.Config = u
}
