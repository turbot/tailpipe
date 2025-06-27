package config

import (
	"github.com/turbot/pipe-fittings/v2/hclhelpers"
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

func NewSource(sourceType string) *Source {
	return &Source{
		Type: sourceType,
		Config: &HclBytes{
			Hcl:   []byte{},
			Range: hclhelpers.Range{},
		},
	}
}
func (s *Source) ToProto() *proto.ConfigData {
	var hcl []byte
	if s.Config != nil {
		hcl = s.Config.Hcl
	}
	return &proto.ConfigData{
		Target: "source." + s.Type,
		Hcl:    hcl,
		Range:  proto.RangeToProto(s.Config.Range.HclRange()),
	}
}

func (s *Source) SetConfigHcl(u *HclBytes) {
	s.Config = u
}
