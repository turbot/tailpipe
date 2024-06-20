package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
)

/* Destination are where logs will be stored after collection.` It's a directory
that will contain many parquet files for different tables etc.

destination "parquet" "local_storage" {
    path = "/path/to/storage/files"
}

A destination is not required for setup, a default one is used.
By default files are saved in the current directory where tailpipe is run.
destination "parquet" "default" {
    path = "."
}

Destinations are normally local, but can use object storage. Obviously remote
storage will be slower, but it's a good way to store at scale.

destination "parquet" "my_s3_bucket" {
    credential = credential.aws.aws_org_root
    path = "s3://my-bucket/path/to/files"
}
*/

// Destination are where logs will be stored after collection.` It's a directory
// that will contain many parquet files for different tables etc.
type Destination struct {
	modconfig.HclResourceImpl

	Type       string
	Path       string      `hcl:"path"`
	Credential *Credential `hcl:"credential"`
}

func NewDestination(block *hcl.Block, fullName string) (modconfig.HclResource, hcl.Diagnostics) {
	if len(block.Labels) < 2 {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "'destination' block requires 2 labels, 'type' and 'name'",
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}

	return &Destination{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fullName),
		Type:            block.Labels[0],
	}, nil
}
