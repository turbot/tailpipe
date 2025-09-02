package database

import (
	"fmt"
	"regexp"
	"strings"
)

// SanitizeDuckDBIdentifier ensures that SQL identifiers (like table or column names)
// are safely quoted using double quotes and escaped appropriately.
//
// The function uses a two-tier approach:
//  1. Simple identifiers (letters, digits, underscore, starting with letter/underscore)
//     are returned unquoted for readability
//  2. Complex identifiers are safely quoted and escaped
//
// For example:
//
//	input:  my_table         → output:  my_table        (unquoted - simple identifier)
//	input:  some"col         → output:  "some""col"     (quoted - contains quote)
//	input:  select           → output:  select          (unquoted - reserved keyword handled by quoting)
//	input:  table with spaces → output: "table with spaces" (quoted - contains spaces)
//
// TODO move to pipe-helpers https://github.com/turbot/tailpipe/issues/517
func SanitizeDuckDBIdentifier(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("empty identifier name")
	}

	// Option 1: allow only simple unquoted identifiers (letters, digits, underscore).
	// Start must be a letter or underscore.
	identRe := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	if identRe.MatchString(name) {
		// Safe to return bare.
		return name, nil
	}

	// Option 2: allow quoting, but escape embedded quotes.
	if strings.Contains(name, "\x00") {
		return "", fmt.Errorf("invalid identifier name: contains NUL")
	}
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`, nil
}

// EscapeLiteral safely escapes SQL string literals for use in WHERE clauses,
// INSERTs, etc. It wraps the string in single quotes and escapes any internal
// single quotes by doubling them.
//
// For example:
//
//	input:  O'Reilly         → output:  'O''Reilly'
//	input:  2025-08-01       → output:  '2025-08-01'
//
// TODO move to pipe-helpers https://github.com/turbot/tailpipe/issues/517
func EscapeLiteral(literal string) string {
	escaped := strings.ReplaceAll(literal, `'`, `''`)
	return `'` + escaped + `'`
}
