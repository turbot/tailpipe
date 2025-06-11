package parquet

import (
	"fmt"
	"regexp"
	"strings"
)

// normalizeExpression processes a config value for use in SQL.
// It safely escapes and quotes strings, but passes through valid SQL expressions and column names.
func normalizeExpression(expr string) (string, error) {
	trimmed := strings.TrimSpace(expr)

	// Case 1: already quoted SQL string literal
	if strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'") && len(trimmed) >= 2 {
		inner := trimmed[1 : len(trimmed)-1]
		escaped := strings.ReplaceAll(inner, "'", "''")
		return fmt.Sprintf("'%s'", escaped), nil
	}

	// Case 2: looks like SQL expression
	if looksLikeSQLExpression(trimmed) {
		return trimmed, nil
	}

	// Case 3: bare identifier (column name), e.g., timestamp, event_id
	if isBareIdentifier(trimmed) {
		return trimmed, nil
	}

	// Case 4: fallback — treat as string literal
	escaped := strings.ReplaceAll(trimmed, "'", "''")
	return fmt.Sprintf("'%s'", escaped), nil
}

func looksLikeSQLExpression(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))

	// Heuristics: contains operators or function-style tokens
	if strings.ContainsAny(s, "()*/%+-=<>|") || strings.Contains(s, "::") {
		return true
	}

	// Known SQL keywords or functions
	sqlExprPattern := regexp.MustCompile(`(?i)\b(select|case|when|then|else|end|cast|concat|coalesce|nullif|date_trunc|extract)\b`)
	if sqlExprPattern.MatchString(s) {
		return true
	}

	// If it contains multiple words (e.g., space), and doesn't match other rules, treat it as not an expression
	if strings.Contains(s, " ") {
		return false
	}

	return false
}

// isBareIdentifier returns true if s is a simple SQL identifier (e.g., a column name).
func isBareIdentifier(s string) bool {
	identifierPattern := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	return identifierPattern.MatchString(s)
}
