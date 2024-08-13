package parse

import (
	"bytes"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/turbot/pipe-fittings/modconfig"
)

func handleUnknownHcl(block *hcl.Block, parseCtx *ConfigParseContext, resource modconfig.HclResource, diags hcl.Diagnostics) hcl.Diagnostics {
	type UnknownHclHandler interface {
		SetUnknownHcl(map[*hclsyntax.Block][]byte) hcl.Diagnostics
	}

	if unknownHclHandler, ok := resource.(UnknownHclHandler); ok {
		var unknown = map[*hclsyntax.Block][]byte{}

		// call sdk handleUnsupportedArgDiags to extract unknown hcl
		hclBytes := parseCtx.FileData[block.DefRange.Filename]
		unknown, diags = handleUnsupportedArgDiags(block.Body, hclBytes, diags)
		moreDiags := unknownHclHandler.SetUnknownHcl(unknown)
		diags = append(diags, moreDiags...)
	}
	return diags
}

func handleUnsupportedArgDiags(body hcl.Body, hclBytes []byte, decodeDiags hcl.Diagnostics) (map[*hclsyntax.Block][]byte, hcl.Diagnostics) {
	var diags hcl.Diagnostics

	var unknownHclMap = map[*hclsyntax.Block][][]byte{}
	// for each  "Unsupported argument" diag, extract the unknown hcl and remove the diag
	for _, diag := range decodeDiags {
		if diag.Severity == hcl.DiagError && diag.Summary == "Unsupported argument" {
			// extract the unknown hcl
			u, block := extractUnknownHcl(body, hclBytes, diag.Subject)
			// if we succeded in extracting the unknown hcl, add it to the list
			if u != nil {

				unknownHclMap[block] = append(unknownHclMap[block], u)
				continue
			}
			// otherwise fall through to add the error
		}
		diags = append(diags, diag)
	}

	unknown := make(map[*hclsyntax.Block][]byte)
	for k, v := range unknownHclMap {
		unknown[k] = bytes.Join(v, []byte("\n"))
	}
	return unknown, diags
}

func extractUnknownHcl(body hcl.Body, hclBytes []byte, subject *hcl.Range) (unknownHcl []byte, b *hclsyntax.Block) {
	// get the start and end positions of the unknown hcl property name
	start := subject.Start
	end := subject.End

	// extract the unknown hcl
	syntaxBody := body.(*hclsyntax.Body)
	var attr *hclsyntax.Attribute

	// get the property name
	property := string(hclBytes[start.Byte:end.Byte])

	// is this attribute within a block
	b = getBlockFoRange(body, subject)
	if b != nil {
		syntaxBody = b.Body
	}

	// now look for this attribute in the body
	attr, ok := syntaxBody.Attributes[property]
	if !ok {
		return nil, nil
	}
	// now get the bytes for the attribute
	unknownHcl = hclBytes[attr.Range().Start.Byte:attr.Range().End.Byte]

	return unknownHcl, b
}

func getBlockFoRange(body hcl.Body, subject *hcl.Range) *hclsyntax.Block {
	b, ok := body.(*hclsyntax.Body)
	if !ok {
		return nil
	}

	for _, block := range b.Blocks {
		if block.Range().Start.Byte <= subject.Start.Byte && block.Range().End.Byte >= subject.End.Byte {
			return block
		}
	}
	return nil
}
