package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_normalizeExpression(t *testing.T) {
	type args struct {
		expr string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "already quoted string literal",
			args: args{
				expr: "'hello world'",
			},
			want: "'hello world'",
		},
		{
			name: "quoted string with single quote",
			args: args{
				expr: "'hello'world'",
			},
			want: "'hello''world'",
		},
		{
			name: "SQL expression with operators",
			args: args{
				expr: "column1 + column2",
			},
			want: "column1 + column2",
		},
		{
			name: "SQL expression with function",
			args: args{
				expr: "COALESCE(column1, 'default')",
			},
			want: "COALESCE(column1, 'default')",
		},
		{
			name: "bare identifier",
			args: args{
				expr: "event_id",
			},
			want: "event_id",
		},
		{
			name: "bare identifier with underscore",
			args: args{
				expr: "user_name_123",
			},
			want: "user_name_123",
		},
		{
			name: "string literal needs quoting",
			args: args{
				expr: "hello world",
			},
			want: "'hello world'",
		},
		{
			name: "string literal with single quote needs escaping",
			args: args{
				expr: "hello'world",
			},
			want: "'hello''world'",
		},
		{
			name: "empty string",
			args: args{
				expr: "",
			},
			want: "''",
		},
		{
			name: "whitespace only",
			args: args{
				expr: "   ",
			},
			want: "''",
		},
		{
			name: "complex SQL expression with multiple operators",
			args: args{
				expr: "(column1 + column2) * (column3 - column4)",
			},
			want: "(column1 + column2) * (column3 - column4)",
		},
		{
			name: "SQL expression with type cast",
			args: args{
				expr: "column1::timestamp",
			},
			want: "column1::timestamp",
		},
		{
			name: "SQL expression with CASE statement",
			args: args{
				expr: "CASE WHEN column1 > 0 THEN 'positive' ELSE 'negative' END",
			},
			want: "CASE WHEN column1 > 0 THEN 'positive' ELSE 'negative' END",
		},
		{
			name: "string with special characters",
			args: args{
				expr: "hello\nworld\twith\rspecial chars",
			},
			want: "'hello\nworld\twith\rspecial chars'",
		},
		{
			name: "string with unicode characters",
			args: args{
				expr: "hello 世界",
			},
			want: "'hello 世界'",
		},
		{
			name: "SQL expression with date function",
			args: args{
				expr: "date_trunc('day', timestamp_column)",
			},
			want: "date_trunc('day', timestamp_column)",
		},
		{
			name: "SQL expression with multiple functions",
			args: args{
				expr: "COALESCE(NULLIF(column1, ''), 'default')",
			},
			want: "COALESCE(NULLIF(column1, ''), 'default')",
		},
		{
			name: "identifier starting with underscore",
			args: args{
				expr: "_private_column",
			},
			want: "_private_column",
		},
		{
			name: "identifier with numbers",
			args: args{
				expr: "column_123",
			},
			want: "column_123",
		},
		{
			name: "string with multiple single quotes",
			args: args{
				expr: "O'Reilly's book",
			},
			want: "'O''Reilly''s book'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeSqlExpression(tt.args.expr)

			assert.Equalf(t, tt.want, got, "NormalizeSqlExpression(%v)", tt.args.expr)
		})
	}
}

func TestIsColumnName(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "valid simple column name",
			args: args{
				s: "column1",
			},
			want: true,
		},
		{
			name: "valid column name with underscore",
			args: args{
				s: "user_name",
			},
			want: true,
		},
		{
			name: "valid column name starting with underscore",
			args: args{
				s: "_private_column",
			},
			want: true,
		},
		{
			name: "valid column name with numbers",
			args: args{
				s: "column_123",
			},
			want: true,
		},
		{
			name: "empty string",
			args: args{
				s: "",
			},
			want: false,
		},
		{
			name: "starts with number",
			args: args{
				s: "1column",
			},
			want: false,
		},
		{
			name: "contains special characters",
			args: args{
				s: "column@name",
			},
			want: false,
		},
		{
			name: "contains spaces",
			args: args{
				s: "column name",
			},
			want: false,
		},
		{
			name: "reserved keyword - select",
			args: args{
				s: "select",
			},
			want: false,
		},
		{
			name: "reserved keyword - from",
			args: args{
				s: "from",
			},
			want: false,
		},
		{
			name: "reserved keyword - where",
			args: args{
				s: "where",
			},
			want: false,
		},
		{
			name: "reserved keyword - case insensitive",
			args: args{
				s: "SELECT",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsColumnName(tt.args.s); got != tt.want {
				t.Errorf("IsColumnName() = %v, want %v", got, tt.want)
			}
		})
	}
}
