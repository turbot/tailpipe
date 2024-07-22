package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
)

/*

// TODO - Is the source typed? Or is it the table? The source should NOT be typed, e.g. one S3 bucket can be used for many different sets of logs since they all have different prefixes.
// TODO - are there destination / destination table types? e.g. http log which accepts Apache + nginx sources
// TODO - Should this be collector or collection or table? Parquet calls it a dataset?
collection "aws_cloudtrail_log" "production" {

    # Source of the data for this collection (required)
    source = source.aws_s3.logs

    # Optional destination for the collection. Uses the parquet default by default.
    # destination = destination.parquet.default

    # Collections may be enabled or disabled. If disabled, they will not collect
    # logs but will still be available for querying logs that have already been
    # collected.
    enabled = true

    # Each collection type may have specific attributes. For example, AWS CloudTrail
    # has a prefix that can be used to be more specific on the source. Optional.
    prefix = "logs/production/"

    # Filters are used to limit the logs that are collected. They are optional.
    # They are run in the order they are defined. A row must pass all filters
    # to be collected.
    # For example, this filter will exclude all decrypt events from KMS which are
    # noisy and rarely useful in log analysis.
    filter {
        where = "not (event_source = 'kms.amazonaws.com' and event_name = 'Decrypt')"
    }

}

# A special collection of root events only
collection "aws_cloudtrail_log" "root_events" {
    source = source.aws_s3.logs
    filter {
        where = "user_identity.type = 'Root'"
    }
}

source "file" "nginx" {
    path = "/var/log/nginx/access.log"
}

collection "http_log" "production" {
    source = source.file.nginx
    format = "combined"
}

collection "custom" "production" {
    source = source.file.nginx
    table_name = "my_custom_table"

    // Format is a regular expression with named capture groups that map to table columns.
    // In this case, we demonstrate formatting of combined http log format.
    // Use a string not double quoted, avoiding many backslashes.
    format = `(?P<remote_addr>\S+) - (?P<remote_user>\S+) \[(?P<time_local>[^\]]+)\] "(?P<request>[^"]+)" (?P<status>\d+) (?P<body_bytes_sent>\d+) "(?P<http_referer>[^"]+)" "(?P<http_user_agent>[^"]+)"`
}
*/

type Collection struct {
	modconfig.HclResourceImpl

	// collection type
	Type string
	// Plugin used for this collection
	// TODO update doc/check
	Plugin string `hcl:"plugin"`

	// Source of the data for this collection (required)
	Source Source `hcl:"source"`
	//Optional destination for the collection. Uses the parquet default by default.
	Destination *Destination `hcl:"destination"`
	// Collections may be enabled or disabled. If disabled, they will not collect
	// logs but will still be available for querying logs that have already beens collected.
	Enabled bool `hcl:"enabled"`
	// Each collection type may have specific attributes. For example, AWS CloudTrail
	// has a prefix that can be used to be more specific on the source. Optional.
	Prefix *string `hcl:"prefix"`
	// Filters are used to limit the logs that are collected. They are optional.
	// They are run in the order they are defined. A row must pass all filters
	// to be collected.
	// For example, this filter will exclude all decrypt events from KMS which are
	// noisy and rarely useful in log analysis.
	Filter    *Filter `hcl:"filter"`
	TableName *string `hcl:"table_name"`

	// Format is a regular expression with named capture groups that map to table columns.
	// In this case, we demonstrate formatting of combined http log format.
	// Use a string not double quoted, avoiding many backslashes.
	Format *string `hcl:"format"`

	Credential *Credential `hcl:"credential"`

	Config []byte
}

func NewCollection(block *hcl.Block, fullName string) (modconfig.HclResource, hcl.Diagnostics) {
	if len(block.Labels) < 2 {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "'collection' block requires 2 labels, 'type' and 'name'",
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}
	return &Collection{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fullName),
		Type:            block.Labels[0],
	}, nil
}
