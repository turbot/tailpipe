package parquet

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_normalizeExpression(t *testing.T) {
	type args struct {
		expr string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "already quoted string literal",
			args: args{
				expr: "'hello world'",
			},
			want:    "'hello world'",
			wantErr: assert.NoError,
		},
		{
			name: "quoted string with single quote",
			args: args{
				expr: "'hello'world'",
			},
			want:    "'hello''world'",
			wantErr: assert.NoError,
		},
		{
			name: "SQL expression with operators",
			args: args{
				expr: "column1 + column2",
			},
			want:    "column1 + column2",
			wantErr: assert.NoError,
		},
		{
			name: "SQL expression with function",
			args: args{
				expr: "COALESCE(column1, 'default')",
			},
			want:    "COALESCE(column1, 'default')",
			wantErr: assert.NoError,
		},
		{
			name: "bare identifier",
			args: args{
				expr: "event_id",
			},
			want:    "event_id",
			wantErr: assert.NoError,
		},
		{
			name: "bare identifier with underscore",
			args: args{
				expr: "user_name_123",
			},
			want:    "user_name_123",
			wantErr: assert.NoError,
		},
		{
			name: "string literal needs quoting",
			args: args{
				expr: "hello world",
			},
			want:    "'hello world'",
			wantErr: assert.NoError,
		},
		{
			name: "string literal with single quote needs escaping",
			args: args{
				expr: "hello'world",
			},
			want:    "'hello''world'",
			wantErr: assert.NoError,
		},
		{
			name: "empty string",
			args: args{
				expr: "",
			},
			want:    "''",
			wantErr: assert.NoError,
		},
		{
			name: "whitespace only",
			args: args{
				expr: "   ",
			},
			want:    "''",
			wantErr: assert.NoError,
		},
		{
			name: "complex SQL expression with multiple operators",
			args: args{
				expr: "(column1 + column2) * (column3 - column4)",
			},
			want:    "(column1 + column2) * (column3 - column4)",
			wantErr: assert.NoError,
		},
		{
			name: "SQL expression with type cast",
			args: args{
				expr: "column1::timestamp",
			},
			want:    "column1::timestamp",
			wantErr: assert.NoError,
		},
		{
			name: "SQL expression with CASE statement",
			args: args{
				expr: "CASE WHEN column1 > 0 THEN 'positive' ELSE 'negative' END",
			},
			want:    "CASE WHEN column1 > 0 THEN 'positive' ELSE 'negative' END",
			wantErr: assert.NoError,
		},
		{
			name: "string with special characters",
			args: args{
				expr: "hello\nworld\twith\rspecial chars",
			},
			want:    "'hello\nworld\twith\rspecial chars'",
			wantErr: assert.NoError,
		},
		{
			name: "string with unicode characters",
			args: args{
				expr: "hello 世界",
			},
			want:    "'hello 世界'",
			wantErr: assert.NoError,
		},
		{
			name: "SQL expression with date function",
			args: args{
				expr: "date_trunc('day', timestamp_column)",
			},
			want:    "date_trunc('day', timestamp_column)",
			wantErr: assert.NoError,
		},
		{
			name: "SQL expression with multiple functions",
			args: args{
				expr: "COALESCE(NULLIF(column1, ''), 'default')",
			},
			want:    "COALESCE(NULLIF(column1, ''), 'default')",
			wantErr: assert.NoError,
		},
		{
			name: "identifier starting with underscore",
			args: args{
				expr: "_private_column",
			},
			want:    "_private_column",
			wantErr: assert.NoError,
		},
		{
			name: "identifier with numbers",
			args: args{
				expr: "column_123",
			},
			want:    "column_123",
			wantErr: assert.NoError,
		},
		{
			name: "string with multiple single quotes",
			args: args{
				expr: "O'Reilly's book",
			},
			want:    "'O''Reilly''s book'",
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeExpression(tt.args.expr)
			if !tt.wantErr(t, err, fmt.Sprintf("normalizeExpression(%v)", tt.args.expr)) {
				return
			}
			assert.Equalf(t, tt.want, got, "normalizeExpression(%v)", tt.args.expr)
		})
	}
}
