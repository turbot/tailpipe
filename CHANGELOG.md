## v0.3.3 [tbd]
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

