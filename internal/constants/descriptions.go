package constants

const (
	TailpipeShortDescription = "Collect, store and analyze log data using SQL"
	TailpipeLongDescription  = `
Tailpipe: select * from logs;
	
Tailpipe is a high-performance data collection and querying tool that makes it 
easy to collect, store, and analyze log data. With Tailpipe you can:

  - Collect logs from various sources and store them efficiently
  - Query your data with familiar SQL syntax using Tailpipe (or DuckDB!)
  - Use Powerpipe to visualize your logs and run detections

Common commands:

  # Install a plugin from the hub - https://hub.tailpipe.io
  tailpipe plugin install aws

  # Run a collection
  tailpipe collect aws_cloudtrail_log.cloudtrail_logs

  # Execute a defined SQL query
  tailpipe query "select * from aws_cloudtrail_log"

  # Get help for a command
  tailpipe help query

Documentation:
  https://tailpipe.io/docs
`
)
