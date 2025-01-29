package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/v2/hclhelpers"
)

type HclBytes struct {
	Hcl   []byte           `cty:"hcl"`
	Range hclhelpers.Range `cty:"range"`
}

func HclBytesForRange(sourceHcl []byte, r hcl.Range) *HclBytes {
	//  get the bytes for the attribute (note: clone the byte buffer)
	hclForRange := append([]byte{}, sourceHcl[r.Start.Byte:r.End.Byte]...)
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
