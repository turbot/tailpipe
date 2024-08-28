package parse

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/tailpipe/internal/config"
)

func extractUnknownHcl(hclBytes []byte, attr *hcl.Attribute) (unknownHcl *config.UnknownHcl) {
	// now get the bytes for the attribute
	unknownHcl = &config.UnknownHcl{
		Hcl:   hclBytes[attr.Range.Start.Byte:attr.Range.End.Byte],
		Range: attr.Range,
	}

	return unknownHcl
}
