## v0.6.2 [2025-07-24]
_Bug fixes_
* Fix issue where `--to` was not respected for zero granularity data. ([#483](https://github.com/turbot/tailpipe/issues/483))
* Fix issue where the relative time passed to `from/to` args were getting parsed incorrectly. ([#485](https://github.com/turbot/tailpipe/issues/485))
* Fix issue where Tailpipe was crashing if the collection state file had nil trunk states from the previous collection. ([#489](https://github.com/turbot/tailpipe/issues/489))
* Fix `.inspect` output to show the plugin name for custom tables. ([#360](https://github.com/turbot/tailpipe/issues/360))
* Fix query JSON outputs to be consistent with DuckDB. ([#432](https://github.com/turbot/tailpipe/issues/432))

_Dependencies_
* Upgrade `go-viper/mapstructure/v2` and `oauth2` packages to remediate high and moderate vulnerabilities.

## v0.6.1 [2025-07-02]
_Bug fixes_
* Update core version to v0.2.9 - fix issue where collection state is not being saved for zero granularity collection. ([#251](https://github.com/turbot/tailpipe-plugin-sdk/issues/251))

## v0.6.0 [2025-07-02]
_What's new_
* Add `--to` flag for `collect`, allowing collection of standalone time ranges. ([#238](https://github.com/turbot/tailpipe/issues/238))
* Add `--overwrite` flag for `collect`, allowing recollection of existing data. ([#454](https://github.com/turbot/tailpipe/issues/454))

_Bug fixes_
* Fix issue where collection state end-objects are cleared when collection is complete, 
meaning no further data will be collected for that day. ([#250](https://github.com/turbot/tailpipe-plugin-sdk/issues/250))

_Behaviour Change_

When passing a `from` time to a collection, the existing partition data is no longer cleared before the collection starts. 
This means that data will not by default be recollected for time ranges that have already been collected. 
To recollect data for a time range, pass the new `--overwrite` flag to the `collect` command.

## v0.5.0 [2025-06-20]
_What's new_
* Added `tp_index` property to partition HCL. Use this to specify the source column for the `tp_index`.  ([#414](https://github.com/turbot/tailpipe/issues/414))
* Updated collection to apply the configured `tp_index`, or `default` if no `tp_index` is specified in the config. 
* Added `--reindex` arg to `compact`. When set, compact will reindex the partition using configured `tp_index` value. ([#413](https://github.com/turbot/tailpipe/issues/413))
  - Removed the partition argument from compact and replaced it with a positional argument.
  - Updated `compact` cleanup to delete empty folders.
* `collect` now always validates required columns are present. (Previously this was only done for custom tables.) ([#411](https://github.com/turbot/tailpipe/issues/411))
  
## v0.4.2 [2025-06-05]
_What's new_
* Enabled support for collecting only today's logs during log collection. ([#394](https://github.com/turbot/tailpipe/issues/394))
* Show available table names in autocomplete for DuckDB meta queries in interactive prompt. ([#357](https://github.com/turbot/tailpipe/issues/357))
* `.inspect` meta-command now shows `tp_` columns at the end. ([#401](https://github.com/turbot/tailpipe/issues/401))

## v0.4.1 [2025-05-19]
_Bug fixes_
* Update `MinCorePluginVersion` to v0.2.5.
* Fix issue where the core plugin was incorrectly throttling the downloads if no temp size limit was specified. ([#204](https://github.com/turbot/tailpipe-plugin-sdk/issues/204))

## v0.4.0 [2025-05-16]
_What's new_
* Add support for memory and temp storage limits for CLI and plugins. ([#396](https://github.com/turbot/tailpipe/issues/396), [#397](https://github.com/turbot/tailpipe/issues/397))
  * `memory_max_mb` controls CLI memory usage and conversion worker count and memory allocation.
  * `plugin_memory_max_mb` controls a per-plugin soft memory cap.
  * `temp_dir_max_mb` limits size of temp data written to disk during a conversion.
  * conversion worker count is now based on memory limit, if set.
  * JSONL to Parquet conversion is now executed in multiple passes, limiting the number of distinct partition keys per conversion.
* Detect and report when a plugin crashes. ([#341](https://github.com/turbot/tailpipe/issues/341))
* Update `show source` output to include source properties. ([#388](https://github.com/turbot/tailpipe/issues/388))

_Bug fixes_
* Fix issue where tailpipe was mentioning steampipe in one of the error messages. ([#389](https://github.com/turbot/tailpipe/issues/389))

## v0.3.2 [2025-04-25]
_What's new_
* Update `MinCorePluginVersion` to v0.2.2.
* Update tailpipe-plugin-sdk to v0.4.0.

_Bug fixes_
* Fix source file error for custom tables when using S3 or other external sources. ([#188](https://github.com/turbot/tailpipe-plugin-sdk/issues/188)) 

## v0.3.1 [2025-04-18]
_Bug fixes_
* Fix partition filter argument. ([#375](https://github.com/turbot/tailpipe/issues/375))

## v0.3.0 [2025-04-16]

_What's new_
* Add support for custom tables. (([#225](https://github.com/turbot/tailpipe/issues/225)))
* Add location to format list/show. ([#283](https://github.com/turbot/tailpipe/issues/283))
* Add plugin to source list/show. ([#337](https://github.com/turbot/tailpipe/issues/337))
* Improve display logic of conversion errors, since we log full error just display first line of error.

_Bug fixes_
* Plugin list command now correctly cases keys in JSON output 
* Add support to querydisplay.ColumnValueAsString for UUID/Decimal in format received from DuckDB
* Add comma separators to numeric output in query results. ([#685](https://github.com/turbot/pipe-fittings/issues/685))
* Fix multiple backtick escaping - if more than 2 properties are back-ticked, the third onwards not being escaped.

## v0.2.1 [2025-04-02]
_Bug fixes_
* Table introspection in json now displays descriptions correctly. ([#323](https://github.com/turbot/tailpipe/issues/323)):
* Fix resolution of the format before calling Collect when the format plugin is different from the table plugin.([#319](https://github.com/turbot/tailpipe/issues/319)):

## v0.2.0 [2025-04-02]
_What's new_
* Add support for plugins with custom formats  
  A `format` block can be defined in config and plugins can provide formats `types` and `presets` . Format are supported by the new Nginx and Apache plugins.
* Add `format list` and `format show` commands. ([#235](https://github.com/turbot/tailpipe/issues/235))
* Update the `plugin show` command to add exported formats and correctly display partitions, etc. ([#257](https://github.com/turbot/tailpipe/issues/257))
* Improve the error reporting in the collection UI ([#247](https://github.com/turbot/tailpipe/issues/247)).
* Update all SQL in code and messaging to be lower case.

## v0.1.3 [2025-03-17]
_What's new_
- Parquet creation is now transactional - write to temp file first then rename. ([#41](https://github.com/turbot/tailpipe/issues/41))
- Views are now created with QuotedIdentifiers allowing use of reserved keywords, etc. ([#205](https://github.com/turbot/tailpipe/issues/205))
- Updated status text for a collection with no active progress. ([#210](https://github.com/turbot/tailpipe/issues/210))

_Bug fixes_
- Fix issue where tailpipe was failing to install private turbot-hosted plugins. ([#201](https://github.com/turbot/tailpipe/issues/201))
- Sort columns before creating view to ensure they're correctly ordered. ([#59](https://github.com/turbot/tailpipe/issues/59))
- DeleteParquetFiles only prunes the partition folder. ([#227](https://github.com/turbot/tailpipe/issues/227))

## v0.1.2 [2025-02-21]
_What's new_
- The `--from` and `--to` arguments now support a wider range of time formats. ([#223](https://github.com/turbot/tailpipe/issues/223))

_Bug fixes_
- Fixes issue where passing the sql file name to tailpipe query was not working. ([#216](https://github.com/turbot/tailpipe/issues/216))
- Fixes issue where tailpipe was failing to install private turbot-hosted plugins. ([#201](https://github.com/turbot/tailpipe/issues/201))

## v0.1.1 [2025-02-10]
_Bug fixes_
- The `table show` command now correctly shows table description. ([#181](https://github.com/turbot/tailpipe/issues/181))
- Fixes issue where tailpipe plugin install/update failed if docker-credential-desktop was found not on PATH. ([#197](https://github.com/turbot/tailpipe/issues/197))

## v0.1.0 [2025-01-30]

Introducing Tailpipe, a high-performance data collection and querying tool that makes it easy to collect, store, and analyze log data.

With Tailpipe you can:

* Collect logs from various sources and store them efficiently
* Query your data with familiar SQL syntax using Tailpipe (or DuckDB!)
* Use Powerpipe to visualize your logs and run detections

Learn more at:
* Website - https://tailpipe.io
* Docs - https://tailpipe.io/docs
* Hub - https://hub.tailpipe.io
* Introduction - https://tailpipe.io/blog/introducing-tailpipe

