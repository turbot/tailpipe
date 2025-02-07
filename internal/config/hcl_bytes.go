package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/v2/hclhelpers"
	"strings"
)

type HclBytes struct {
	Hcl   []byte           `cty:"hcl"`
	Range hclhelpers.Range `cty:"range"`
}

// HclBytesForRange extracts the HCL bytes for a given range from a source HCL byte buffer
// This is used to extract the HCL for a unknown attributes so we can pass it to a plugin to parse
func HclBytesForRange(sourceHcl []byte, r hcl.Range) *HclBytes {
	//  get the bytes for the attribute (note: clone the byte buffer)
	hclForRange := append([]byte{}, sourceHcl[r.Start.Byte:r.End.Byte]...)

	// TACTICAL: if the HCL ends with "EOT" we need to add a newline to ensure the HCL parser doesn't fail
	// with the error "Unterminated template string; No closing marker was found for the string."
	if strings.HasSuffix(string(hclForRange), "EOT") {
		hclForRange = append(hclForRange, '\n')
	}
	return &HclBytes{
		Hcl:   hclForRange,
		Range: hclhelpers.NewRange(r),
	}
}
func (h *HclBytes) Merge(other *HclBytes) {
	if len(h.Hcl) == 0 {
		h.Hcl = other.Hcl
		h.Range = other.Range
		return
	}

	// Append a newline and the new HCL content
	h.Hcl = append(h.Hcl, '\n')
	h.Hcl = append(h.Hcl, other.Hcl...)

	// update the range to encompass the new range
	if other.Range.Start.Byte < h.Range.Start.Byte {
		h.Range.Start = other.Range.Start
	}
	if other.Range.End.Byte > h.Range.End.Byte {
		h.Range.End = other.Range.End
	}
}
