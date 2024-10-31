# Inside Tailpipe
https://github.com/turbot/pipes/wiki/%5BDRAFT%5D-Tailpipe-Core-Concept,-HCL,-CLI-design

## Partitions

## Commands
### Collect

Pseudocode
```go
get partition config
create collector
for each partition {
    collect
}
close collector
```
These should all be valid:
```
...tailpipe aws_cloudtrail_log
# same as:
tailpipe 'aws_cloudtrail_log.*'

# collect a single partition
tailpipe collect aws_cloudtrail_log.prod

# collect all partitions with a given name:
tailpipe collect prod
# same as
tailpipe collect '*.prod'
```

### Plugin
#### List
#### Install
#### Uninstall
#### Update

### Query
#### List
#### Run

### Configuration and Infrastructure
### Tailpipe Config
Contains:
-  map of partitions, keyed by unqualified name (<partition_type>.<partition_name>)
- map of plugin configs, keyed by plugin image ref
-  (for each image ref we store an array of configs)
- map of plugin configs, keyed by plugin instance
- map of installed plugin versions, keyed by plugin image ref

### Database

The CLI creates a duckdb database in `~/.tailpipe/db`.

After a colleciton is complete, a view is created/updated to view that table
TODO more details on view creation -> single view per table/partition

### Partitions

### Connections
Pipeling connections are loaded from the ~/.tailpipe/config folder

### Workspaces
Workspace profiles are loaded from the ~/.tailpipe/config folder

## Collection Process

### Overview of Data Flow:

### Collector

### JSON-Parquet Conversion

### Collection State
