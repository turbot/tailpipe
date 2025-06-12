package config

import (
	"regexp"
	"strings"
)

// IsColumnName returns true if the string is a valid SQL column name.
// It checks that the name:
// 1. Contains only alphanumeric characters and underscores
// 2. Starts with a letter or underscore
// 3. Is not empty
// 4. Is not a DuckDB reserved keyword
func IsColumnName(s string) bool {
	if s == "" {
		return false
	}
	// Match pattern: start with letter/underscore, followed by alphanumeric/underscore
	columnNameRegex := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	if !columnNameRegex.MatchString(s) {
		return false
	}

	// Check for DuckDB reserved keywords
	// This list is based on DuckDB's SQL dialect
	reservedKeywords := map[string]bool{
		"all": true, "alter": true, "and": true, "any": true, "array": true, "as": true,
		"asc": true, "between": true, "by": true, "case": true, "cast": true, "check": true,
		"collate": true, "column": true, "constraint": true, "create": true, "cross": true,
		"current_date": true, "current_time": true, "current_timestamp": true, "database": true,
		"default": true, "delete": true, "desc": true, "distinct": true, "drop": true,
		"else": true, "end": true, "except": true, "exists": true, "extract": true,
		"false": true, "for": true, "foreign": true, "from": true, "full": true,
		"group": true, "having": true, "in": true, "index": true, "inner": true,
		"insert": true, "intersect": true, "into": true, "is": true, "join": true,
		"left": true, "like": true, "limit": true, "natural": true, "not": true,
		"null": true, "on": true, "or": true, "order": true, "outer": true,
		"primary": true, "references": true, "right": true, "select": true, "set": true,
		"some": true, "table": true, "then": true, "to": true, "true": true,
		"union": true, "unique": true, "update": true, "using": true, "values": true,
		"when": true, "where": true, "with": true,
	}

	return !reservedKeywords[strings.ToLower(s)]
}

//
//
//// NormalizeSqlExpression processes a config value for use in SQL.
//// It safely escapes and quotes strings, but passes through valid SQL expressions and column names.
//func NormalizeSqlExpression(expr string) string {
//	trimmed := strings.TrimSpace(expr)
//
//	// NOTE: for now we only
//
//	// Case 1: already quoted SQL string literal
//	if strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'") && len(trimmed) >= 2 {
//		inner := trimmed[1 : len(trimmed)-1]
//		escaped := strings.ReplaceAll(inner, "'", "''")
//		return fmt.Sprintf("'%s'", escaped)
//	}
//
//	// Case 2: looks like SQL expression
//	if looksLikeSQLExpression(trimmed) {
//		return trimmed
//	}
//
//	// Case 3: bare identifier (column name), e.g., timestamp, event_id
//	if isBareIdentifier(trimmed) {
//		return trimmed
//	}
//
//	// Case 4: fallback — treat as string literal
//	escaped := strings.ReplaceAll(trimmed, "'", "''")
//	return fmt.Sprintf("'%s'", escaped)
//}
//
//func looksLikeSQLExpression(s string) bool {
//	s = strings.ToLower(strings.TrimSpace(s))
//
//	// Heuristics: contains operators or function-style tokens
//	if strings.ContainsAny(s, "()*/%+-=<>|") || strings.Contains(s, "::") {
//		return true
//	}
//
//	// Known SQL keywords or functions
//	sqlExprPattern := regexp.MustCompile(`(?i)\b(select|case|when|then|else|end|cast|concat|coalesce|nullif|date_trunc|extract)\b`)
//	if sqlExprPattern.MatchString(s) {
//		return true
//	}
//
//	// If it contains multiple words (e.g., space), and doesn't match other rules, treat it as not an expression
//	if strings.Contains(s, " ") {
//		return false
//	}
//
//	return false
//}
//
//// isBareIdentifier returns true if s is a simple SQL identifier (e.g., a column name).
//func isBareIdentifier(s string) bool {
//	identifierPattern := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
//	return identifierPattern.MatchString(s)
//}
