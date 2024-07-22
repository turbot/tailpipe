# tailpipe

Tailpipe is a simple, open-source and extensible SIEM tool.

## Features

* Ingest logs from multiple sources
* Parse logs into structured data
* Store logs in parquet format with partioning
* Query logs using SQL (DuckDB)
* Filter log data before storage
* Detect issues in real-time during ingestion
* Query over multiple logs at once

## WIP for Developers at 22-July-2024

This is WIP code.

Currelty the on

Currently, it works with Turbot Pipes, so set your token:
```
export PIPES_TOKEN=tpt_example
```

Make and install the required plugins:
```
cd ~/src
git clone git@github.com:turbot/tailpipe-plugin-aws.git
git clone git@github.com:turbot/tailpipe-plugin-pipes.git
git clone git@github.com:turbot/tailpipe-plugin-sdk.git

cd ~/src/tailpipe-plugin-aws
make

cd ~/src/tailpipe-plugin-pipes
make
```

Make and run the program to download audit logs from Pipes and store in Parquet format:
```
cd ~/src/tailpipe
make

```

configure collection config
``` 
# tailpipe.hcl
```

set credentials and execute
```
# AWS
export AWS_ACCESS_KEY_ID="xxxx"
export AWS_SECRET_ACCESS_KEY="xxxx"
# optional
export AWS_SESSION_TOKEN="xxxxx"

# pipes
export PIPES_TOKEN="xxxx"

# run
tailpipe collect collection=aws_cloudtrail_log    

```


Use DuckDB to query the files:

```
duckdb
```

Setup a view and run queries:
```
create view pipes_audit_log as select * from '~/Downloads/tailpipe/*/*/*/*/*/*.parquet';

select action_type, count(*) as total from pipes_audit_log group by all order by total desc;
```









## WIP for Developers at 17-Apr-2024

This is WIP code. Currently `main.go` just runs a simple program to load a
configuration string, load the requested plugins (which need to be pre-built
and installed) and then execute a data sync.

Currently, it works with Turbot Pipes, so set your token:
```
export PIPES_TOKEN=tpt_example
```

Make and install the required plugins:
```
cd ~/src
git clone git@github.com:turbot/tailpipe-plugin-aws.git
git clone git@github.com:turbot/tailpipe-plugin-file.git
git clone git@github.com:turbot/tailpipe-plugin-pipes.git
git clone git@github.com:turbot/tailpipe-plugin-sdk.git

cd ~/src/tailpipe-plugin-aws
make

cd ~/src/tailpipe-plugin-file
make

cd ~/src/tailpipe-plugin-pipes
make
```

Make and run the program to download audit logs from Pipes and store in Parquet format:
```
cd ~/src/tailpipe
make
```

Check the output directory for Parquet files:
```
ls -alR ~/Downloads/tailpipe
```

Use DuckDB to query the files:
```
duckdb
```

Setup a view and run queries:
```
create view pipes_audit_log as select * from '~/Downloads/tailpipe/*/*/*/*/*/*.parquet';

select action_type, count(*) as total from pipes_audit_log group by all order by total desc;
```


## [WIP] CLI commands

```
tailpipe collect - Collect logs from sources.
tailpipe compact - Compact parquet files and truncate data.
tailpipe help - Help about any command.
tailpipe login - Login to Turbot Pipes.
tailpipe mod - Mod management.
tailpipe query - Interactive console to query logs.
tailpipe server - Start a server to perform ongoing collection of logs with detections.
tailpipe source - Source information.
tailpipe table - Table information.
tailpipe view - View information.
```

## [WIP] Parquet and storage

Hive structure:
```
{table_type}/tp_collection={collection}/tp_connection={partition}/tp_year={year}/tp_month={month}/tp_day={day}/{uuid}.parquet
```

(Q - would {collection}/{table} be more efficient and common than {table}/{collection}? I don't think so, more likely to query by table than across multiple types in one collection I think?)

For example, AWS CloudTrail:
```
aws_cloudtrail_log/tp_collection=all_accounts/tp_connection=123456789012/tp_year=2024/tp_month=02/tp_day=07/abc1234.parquet
```

For example, Slack:
```
slack_audit_log/tp_collection=my_collection/tp_connection={tenant_id}/tp_year=2024/tp_month=02/tp_day=07/abc1234.parquet
```

Tailpipe will collect and then compact logs - these are deliberately different
phases.  Collection creates a series of smaller parquet files added to the
specific day directory. Compaction will then combine those files (per-day) into
a single larger file. File changes will be done as temp files with instant
(almost transactional) renaming operations - allowing DuckDB to use the files
with minimal chance of locking / parse errors.

We can easily create views using this structure. They can even go across schemas for flexible
querying similar to steampipe.
```sql
-- view across all log types
create view all_log as select * from read_parquet('*/*/*/*/*/*/*.parquet');

-- view across collections
create view aws_cloudtrail_log as select * from read_parquet('aws_cloudtrail_log/*/*/*/*/*/*.parquet');
create view slack_audit_log as select * from read_parquet('slack_audit_log/*/*/*/*/*/*.parquet');

-- view across one collection
create schema my_collection;
create view my_collection.aws_connection_log as select * from read_parquet('*/tp_collection=my_collection/*/*/*/*/*.parquet');

-- view all logs of all types for one AWS account
create schema my_conn;
create view my_conn.aws_connection_log as select * from read_parquet('*/*/tp_connection=my_conn/*/*/*/*.parquet');

-- create a datatank style accelerated table
create schema datatank;
create table aws_cloudtrail_log as select * from read_parquet('aws_cloudtrail_log/*/*/*/*/*/*.parquet');
set search_path = 'datatank,main';
```

Dynamic views might be interesting, especially if they improve performance querying specific hive partitions. For example:
```sql
create schema period_2024_q1;
create view period_2024_q1.aws_cloudtrail_log as select * from read_parquet('aws_cloudtrail_log/*/*/tp_year=2024/tp_month=01/*/*.parquet', 'aws_cloudtrail_log/*/*/tp_year=2024/tp_month=02/*/*.parquet', 'aws_cloudtrail_log/*/*/tp_year=2024/tp_month=03/*/*.parquet');
```

## [WIP] HCL configuration

Tailpipe uses HCL for configuration, like all open source tools in the Pipes family.

Questions:
* Multiple schemas? (I think yes)
* Multiple tables per schema? (I think yes)
* Can a table schema (e.g. aws_cloudtrail) have multiple per schema? Annoying for naming tables, I'm not sure about this one.
* Can a table have multiple collectors? Yes, but they must have the same table schema.
* Can a collector have multiple filters? How are they combined? Yes, they are and-ed.
* Can a single source (e.g. an S3 bucket) send data to multiple tables? Or is it (re-)scanned separately for each. Clearly it would be more efficient to say yes, but it will make it more complicated on this dimension and I'm not sure how important it is to support.

```hcl
# Credentials are used to store secret information only, similar to flowpipe.
credential "aws" "aws_org_root" {
    role = "..."
}

# Destinations are where logs will be stored after collection. It's a directory
# that will contain many parquet files for different tables etc.
destination "parquet" "local_storage" {
    path = "/path/to/storage/files"
}

# A destination is not required for setup, a default one is used.
# By default files are saved in the current directory where tailpipe is run.
destination "parquet" "default" {
    path = "."
}

# Destinations are normally local, but can use object storage. Obviously remote
# storage will be slower, but it's a good way to store at scale.
destination "parquet" "my_s3_bucket" {
    credential = credential.aws.aws_org_root
    path = "s3://my-bucket/path/to/files"
}

# Sources are where logs are collected from. They can be local files, remote
# files, or APIs. This source is for an S3 bucket that includes logs.
# Sources are agnostic to the logs that they contain, each source may by used
# by multiple collectors.
source "aws_s3" "logs" {
    bucket = "my-bucket"
    prefix = "logs/" // optional, default is no prefix
    region = "us-west-2"
    credential = credential.aws.aws_org_root
}

# Slack is a simpler source, really just defined through credentials since the
# endpoint is well known.
# TODO - Is this required? Or should collections accept either a source or a credential?
source "slack" "default" {
    credential = credential.slack.default
}

# TODO - Is the source typed? Or is it the table? The source should NOT be typed, e.g. one S3 bucket can be used for many different sets of logs since they all have different prefixes.
# TODO - are there destination / destination table types? e.g. http log which accepts Apache + nginx sources
# TODO - Should this be collector or collection or table? Parquet calls it a dataset?
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
```
