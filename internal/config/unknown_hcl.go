package config

import (
	"github.com/hashicorp/hcl/v2"
)

//// UnknownHclHandler interface which must be implemented by any entity which supports unknown hcl
//type UnknownHclHandler interface {
//	SetUnknownHcl(*UnknownHclMap) hcl.Diagnostics
//}
//
//type UnknownHclMap struct {
//	Unknown *UnknownHcl
//	ChildUnknown map[string]*UnknownHcl
//}
//func NewUnknownHclMap() *UnknownHclMap {
//	return &UnknownHclMap{
//		ChildUnknown: map[string]*UnknownHcl{},
//	}
//}
//
//func (u *UnknownHclMap) Merge(other *UnknownHcl, block *hclsyntax.Block) {
//	if block != nil {
//		if existing := u.ChildUnknown[block.Type]; existing == nil {
//			u.ChildUnknown[block.Type] = other
//		} else {
//			// TODO does this mutate the map???
//			existing.Merge(other)
//		}
//	} else {
//		if u.Unknown == nil {
//			u.Unknown = other
//		} else {
//			u.Unknown.Merge(other)
//		}
//	}
//}

type UnknownHcl struct {
	Hcl   []byte
	Range hcl.Range
}

func (h *UnknownHcl) Merge(other *UnknownHcl) {
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
