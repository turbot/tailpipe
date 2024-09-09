package parse

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/tailpipe/internal/config"
	"log/slog"
)

func extractUnknownHcl(hclBytes []byte, attr *hcl.Attribute) (unknownHcl *config.UnknownHcl) {
	// now get the bytes for the attribute
	unknownHcl = &config.UnknownHcl{
		Hcl:   hclBytes[attr.Range.Start.Byte:attr.Range.End.Byte],
		Range: attr.Range,
	}

	slog.Debug("extracted unknown hcl", "start", attr.Range.Start.Byte, "end", attr.Range.End.Byte, "source", string(hclBytes), "hcl", string(unknownHcl.Hcl))
	return unknownHcl
}
