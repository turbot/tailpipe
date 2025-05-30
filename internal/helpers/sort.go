package helpers

import (
	"slices"
	"strings"

	"github.com/turbot/tailpipe/internal/constants"
)

// SortColumnsAlphabetically sorts the column names alphabetically but with tp_ fields on the end
func SortColumnsAlphabetically(columns []string) []string {
	slices.SortFunc(columns, func(a, b string) int {
		isPrefixedA, isPrefixedB := strings.HasPrefix(a, constants.TpPrefix), strings.HasPrefix(b, constants.TpPrefix)
		switch {
		case isPrefixedA && !isPrefixedB:
			return 1 // a > b
		case !isPrefixedA && isPrefixedB:
			return -1 // a < b
		default:
			return strings.Compare(a, b) // normal alphabetical comparison
		}
	})
	return columns
}
