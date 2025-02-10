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

// HclBytesForLines extracts the HCL bytes for a given range from a source HCL byte buffer,
// ensuring that it includes full lines rather than using byte-based slicing.
func HclBytesForLines(sourceHcl []byte, r hcl.Range) *HclBytes {
	// Convert HCL bytes to a string and split into lines.
	lines := strings.Split(string(sourceHcl), "\n")

	// Ensure the range is within bounds.
	if r.Start.Line < 1 || r.Start.Line > len(lines) || r.End.Line < 1 {
		return &HclBytes{Hcl: []byte{}, Range: hclhelpers.NewRange(r)}
	}

	// Extract full lines from the start to end line (inclusive).
	extractedLines := lines[r.Start.Line-1 : r.End.Line]

	// Reconstruct the HCL snippet from the extracted lines, ensuring a trailing newline.
	hclForRange := []byte(strings.Join(extractedLines, "\n") + "\n")

	// TACTICAL FIX: If the HCL snippet ends with "EOT", add an extra newline to prevent parser errors.
	if strings.HasSuffix(strings.TrimSpace(string(hclForRange)), "EOT") {
		hclForRange = append(hclForRange, '\n')
	}

	return &HclBytes{
		Hcl:   hclForRange,
		Range: hclhelpers.NewRange(r),
	}
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
