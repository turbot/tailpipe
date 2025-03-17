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

