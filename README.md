<a href="https://tailpipe.io"><img width="67%" src="https://tailpipe.io/images/pipeling-wordmarks/tailpipe_wordmark_white_outline.svg" /></a>

[![plugins](https://img.shields.io/badge/plugins-5-blue)](https://hub.tailpipe-io.vercel.app/) &nbsp; 
[![plugins](https://img.shields.io/badge/mods-14-blue)](https://hub.tailpipe-io.vercel.app/) &nbsp; 
[![slack](https://img.shields.io/badge/slack-2695-blue)](https://turbot.com/community/join?utm_id=gspreadme&utm_source=github&utm_medium=repo&utm_campaign=github&utm_content=readme) &nbsp;
[![maintained by](https://img.shields.io/badge/maintained%20by-Turbot-blue)](https://turbot.com?utm_id=gspreadme&utm_source=github&utm_medium=repo&utm_campaign=github&utm_content=readme)

# select * from logs;

[Tailpipe](https://tailpipe.io) is the **lightweight**, **developer-friendly** way to query logs.

**Cloud logs, SQL insights**. Collects logs from cloud, container and application sources. Query and analyze your data instantly with the power of SQL, right from your terminal.

**Fast, local, and efficient**. Runs locally, powered by DuckDB's in-memory analytics and Parquet's optimized storage.

**An ecosystem of prebuilt intelligence**. MITRE ATT&CK-aligned queries, prebuilt detections, benchmarks, and dashboards, all open source and community-driven.

**Built to build with**. Define detections as code, extend functionality with plugins and write custom SQL queries.

## Demo time!

**[Watch on YouTube →](https://www.youtube.com/watch?v=IR9MK1DMvW4)**

<a href="https://www.youtube.com/watch?v=IR9MK1DMvW4"><img alt="tailpipe demo" width=500 src="https://tailpipe.io/images/video_preview.png"></a>

## Documentation

See the [documentation](https://tailpipe.io/docs) for:

- [Getting started](https://tailpipe.io/docs)
- [It's just SQL!](https://tailpipe.io/docs/sql)
- [Managing Tailpipe](https://tailpipe.io/docs/manage)
- [CLI commands](https://tailpipe.io/docs/reference/cli)

Plugins and query examples are on the [Tailpipe Hub](https://hub.tailpipe.io).

Prebuilt detection benchmarks are on the [Powerpipe Hub](https://hub.powerpipe.io/?engines=tailpipe).

## Getting Started

Install Tailpipe from the [downloads](https://tailpipe.io/downloads) page:

```sh
# MacOS
brew install turbot/tap/tailpipe
```

```sh
# Linux or Windows (WSL)
sudo /bin/sh -c "$(curl -fsSL https://tailpipe.io/install/tailpipe.sh)"
```

Install a plugin from the [Tailpipe Hub](https://hub.tailpipe.io) for your favorite service (e.g. [AWS](https://hub.tailpipe.io/plugins/turbot/aws), [Azure](https://hub.tailpipe.io/plugins/turbot/azure), [GCP](https://hub.tailpipe.io/plugins/turbot/gcp)):

```sh
tailpipe plugin install aws
```

Configure your [connection](https://tailpipe.io/docs/manage/connection) credentials, table [partition](https://tailpipe.io/docs/manage/partition) and data [source](https://tailpipe.io/docs/manage/source). Here is an [AWS CloudTrail example](https://hub.tailpipe.io/plugins/turbot/aws/tables/aws_cloudtrail_log#example-configurations):

```sh
vi ~/.tailpipe/config/aws.tpc
```

```hcl
connection "aws" "logging_account" {
  profile = "my-logging-account"
}

partition "aws_cloudtrail_log" "my_logs" {
  source "aws_s3_bucket" {
    connection = connection.aws.logging_account
    bucket     = "aws-cloudtrail-logs-bucket"
  }
}
```

Download, enrich, and save logs from your source ([examples](https://tailpipe.io/docs/reference/cli/collect)):

```sh
tailpipe collect aws_cloudtrail_log
```

Enter interactive query mode:

```sh
tailpipe query
```

Run a query:

```sql
select
  event_source,
  event_name,
  count(*) as event_count
from
  aws_cloudtrail_log
where
  not read_only
group by
  event_source,
  event_name
order by
  event_count desc;
```

```sh
+----------------------+-----------------------+-------------+
| event_source         | event_name            | event_count |
+----------------------+-----------------------+-------------+
| logs.amazonaws.com   | CreateLogStream       | 793845      |
| ecs.amazonaws.com    | RunTask               | 350836      |
| ecs.amazonaws.com    | SubmitTaskStateChange | 190185      |
| s3.amazonaws.com     | PutObject             | 60842       |
| sns.amazonaws.com    | TagResource           | 25499       |
| lambda.amazonaws.com | TagResource           | 20673       |
+----------------------+-----------------------+-------------+
```

## Detections as Code with Powerpipe

Pre-built dashboards and detections for the AWS plugin are available in [Powerpipe](https://powerpipe.io) mods, helping you monitor and analyze activity across your AWS accounts.

For example, the [AWS CloudTrail Logs Detections mod](https://hub.powerpipe.io/mods/turbot/tailpipe-mod-aws-cloudtrail-log-detections) scans your CloudTrail logs for anomalies, such as an S3 bucket being made public or a change in your VPC network infrastructure.

Dashboards and detections are [open source](https://github.com/topics/tailpipe-mod), allowing easy customization and collaboration.

To get started, choose a mod from the [Powerpipe Hub](https://hub.powerpipe.io/?engines=tailpipe).

## Developing

If you want to help develop the core Tailpipe binary, these are the steps to build it.

**Clone**:

```sh
git clone https://github.com/turbot/tailpipe
```

**Build**:

```
cd tailpipe
make
```

**Check the version**:

```
$ tailpipe --version
Tailpipe version 0.1.0
```

## Open source and contributing

This repository is published under the [AGPL 3.0](https://www.gnu.org/licenses/agpl-3.0.html) license. Please see our [code of conduct](https://github.com/turbot/.github/blob/main/CODE_OF_CONDUCT.md). Contributors must sign our [Contributor License Agreement](https://turbot.com/open-source#cla) as part of their first pull request. We look forward to collaborating with you!

[Tailpipe](https://tailpipe.io) is a product produced from this open source software, exclusively by [Turbot HQ, Inc](https://turbot.com). It is distributed under our commercial terms. Others are allowed to make their own distribution of the software, but cannot use any of the Turbot trademarks, cloud services, etc. You can learn more in our [Open Source FAQ](https://turbot.com/open-source).

## Get involved

**[Join #tailpipe on Slack →](https://turbot.com/community/join)**


