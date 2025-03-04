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

