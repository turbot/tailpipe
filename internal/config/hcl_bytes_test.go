package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/v2/hclhelpers"
	"reflect"
	"testing"
)

// ✅ Test for extracting HCL based on line range
func TestHclBytesForLines(t *testing.T) {
	type args struct {
		sourceHcl []byte
		r         hcl.Range
	}

	source := []byte(`resource "aws_s3_bucket" "my_bucket" {
  bucket = "my-bucket-name"
  acl    = "private"
}

output "bucket_name" {
  value = aws_s3_bucket.my_bucket.bucket
}
`)

	tests := []struct {
		name string
		args args
		want *HclBytes
	}{
		{
			name: "Extract single line",
			args: args{
				sourceHcl: source,
				r: hcl.Range{
					Start: hcl.Pos{Line: 2},
					End:   hcl.Pos{Line: 2},
				},
			},
			want: &HclBytes{
				Hcl:   []byte("  bucket = \"my-bucket-name\"\n"),
				Range: hclhelpers.NewRange(hcl.Range{Start: hcl.Pos{Line: 2}, End: hcl.Pos{Line: 2}}),
			},
		},
		{
			name: "Extract multiple lines",
			args: args{
				sourceHcl: source,
				r: hcl.Range{
					Start: hcl.Pos{Line: 2},
					End:   hcl.Pos{Line: 3},
				},
			},
			want: &HclBytes{
				Hcl: []byte(`  bucket = "my-bucket-name"
  acl    = "private"
`),
				Range: hclhelpers.NewRange(hcl.Range{Start: hcl.Pos{Line: 2}, End: hcl.Pos{Line: 3}}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HclBytesForLines(tt.args.sourceHcl, tt.args.r)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HclBytesForRange() = \n%q, want \n%q", got.Hcl, tt.want.Hcl)
			}
		})
	}
}

// ✅ Test for merging HCL byte ranges
func TestHclBytes_Merge(t *testing.T) {
	type fields struct {
		Hcl   []byte
		Range hclhelpers.Range
	}

	type args struct {
		other *HclBytes
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		want   *HclBytes
	}{
		{
			name: "Merge adjacent blocks",
			fields: fields{
				Hcl:   []byte("resource \"aws_s3_bucket\" \"my_bucket\" {\n  bucket = \"my-bucket-name\"\n}"),
				Range: hclhelpers.NewRange(hcl.Range{Start: hcl.Pos{Line: 1}, End: hcl.Pos{Line: 3}}),
			},
			args: args{
				other: &HclBytes{
					Hcl:   []byte("\noutput \"bucket_name\" {\n  value = aws_s3_bucket.my_bucket.bucket\n}"),
					Range: hclhelpers.NewRange(hcl.Range{Start: hcl.Pos{Line: 4}, End: hcl.Pos{Line: 6}}),
				},
			},
			want: &HclBytes{
				Hcl: []byte(`resource "aws_s3_bucket" "my_bucket" {
  bucket = "my-bucket-name"
}
output "bucket_name" {
  value = aws_s3_bucket.my_bucket.bucket
}`),
				Range: hclhelpers.NewRange(hcl.Range{Start: hcl.Pos{Line: 1}, End: hcl.Pos{Line: 6}}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &HclBytes{
				Hcl:   tt.fields.Hcl,
				Range: tt.fields.Range,
			}
			h.Merge(tt.args.other)
			if !reflect.DeepEqual(h, tt.want) {
				t.Errorf("Merge() = \n%q, want \n%q", h.Hcl, tt.want.Hcl)
			}
		})
	}
}
