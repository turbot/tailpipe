package parse

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/tailpipe/internal/config"
)

func extractHclForAttribute(hclBytes []byte, attr *hcl.Attribute) (unknownHcl *config.HclBytes) {
	//  get the bytes for the attribute (note: clone the byte buffer)
	hcl := append([]byte{}, hclBytes[attr.Range.Start.Byte:attr.Range.End.Byte]...)
	unknownHcl = &config.HclBytes{
		Hcl:   hcl,
		Range: attr.Range,
	}

	return unknownHcl
}
