package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/v2/hclhelpers"
	"reflect"
	"testing"
)

// Test for extracting HCL based on line range
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

// Test for merging HCL byte ranges
func TestHclBytes_Merge(t *testing.T) {
	type args struct {
		b     *HclBytes
		other *HclBytes
	}
	tests := []struct {
		name string
		args args
		want *HclBytes
	}{
		{
			name: "Merge adjacent blocks",

			args: args{
				b: &HclBytes{
					Hcl: []byte(`resource "aws_s3_bucket" "my_bucket" {
  bucket = "my-bucket-name"
}`),
					Range: hclhelpers.NewRange(hcl.Range{Start: hcl.Pos{Line: 1}, End: hcl.Pos{Line: 3}}),
				},
				other: &HclBytes{
					Hcl: []byte(`
output "bucket_name" {
  value = aws_s3_bucket.my_bucket.bucket
}`),
					Range: hclhelpers.NewRange(hcl.Range{Start: hcl.Pos{Line: 4}, End: hcl.Pos{Line: 6, Byte: 100}}),
				},
			},
			want: &HclBytes{
				Hcl: []byte(`resource "aws_s3_bucket" "my_bucket" {
  bucket = "my-bucket-name"
}

output "bucket_name" {
  value = aws_s3_bucket.my_bucket.bucket
}`),
				Range: hclhelpers.NewRange(hcl.Range{Start: hcl.Pos{Line: 1}, End: hcl.Pos{Line: 6, Byte: 100}}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			tt.args.b.Merge(tt.args.other)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("Merge() = \n%s, want \n%s", tt.args.b, tt.want)
			}
		})
	}
}
