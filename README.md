<a href="https://tailpipe.io"><img width="67%" src="https://tailpipe.io/images/pipeling-wordmarks/tailpipe_wordmark.svg" /></a>

[![plugins](https://img.shields.io/badge/plugins-5-blue)](https://hub.tailpipe-io.vercel.app/) &nbsp; 
[![plugins](https://img.shields.io/badge/mods-14-blue)](https://hub.tailpipe-io.vercel.app/) &nbsp; 
[![slack](https://img.shields.io/badge/slack-2695-blue)](https://turbot.com/community/join?utm_id=gspreadme&utm_source=github&utm_medium=repo&utm_campaign=github&utm_content=readme) &nbsp;
[![maintained by](https://img.shields.io/badge/maintained%20by-Turbot-blue)](https://turbot.com?utm_id=gspreadme&utm_source=github&utm_medium=repo&utm_campaign=github&utm_content=readme)

## select * from logs;

[Tailpipe](https://tailpipe-io.vercel.app) is the **lightweight**, **developer-friendly** way to query logs.

**Cloud logs, SQL insights**. Collects logs from cloud, container and application sources. Query and analyze your data instantly with the power of SQL, right from your terminal.

**Fast, local, and efficient**. Runs locally, powered by DuckDB's in-memory analytics and Parquet's optimized storage.

**An ecosystem of prebuilt intelligence**. MITRE ATT&CK-aligned queries, prebuilt detections, benchmarks, and dashboards, all open source and community-driven.

**Built to build with**. Define detections as code, extend functionality with plugins and write custom SQL queries.

## Demo time!

**[Watch on YouTube →](https://www.youtube.com/watch?v=IR9MK1DMvW4)**

<a href="https://www.youtube.com/watch?v=IR9MK1DMvW4"><img alt="tailpipe demo" width=500 src="https://tailpipe.io/images/tailpipe_hero_video_thumbnail.png"></a>

## Documentation

See the [documentation](https://tailpipe-io.vercel.app/docs) for:

- [Getting started](https://tailpipe-io.vercel.app/docs)
- [It's just SQL!](https://tailpipe-io.vercel.app/docs/sql)
- [Managing Tailpipe](https://tailpipe-io.vercel.app/docs/manage)
- [CLI commands](https://tailpipe-io.vercel.app/docs/reference/cli)

## Install Tailpipe

Install Tailpipe from the [downloads](https://tailpipe-io.vercel.app/downloads) page:

```sh
# MacOS
brew install turbot/tap/tailpipe
```

```
# Linux or Windows (WSL2)
sudo /bin/sh -c "$(curl -fsSL https://tailpipe-io.vercel.app/install/tailpipe.sh)"
```

## Install a plugin

Install a plugin for your favorite service (e.g. [AWS](https://hub.tailpipe-io.vercel.app/plugins/turbot/aws), [Azure](https://hub.tailpipe-io.vercel.app/plugins/turbot/azure), [GCP](https://hub.tailpipe-io.vercel.app/plugins/turbot/gcp), [Pipes](https://hub.tailpipe-io.vercel.app/plugins/turbot/pipes).

```sh
tailpipe plugin install aws
```

## Configure a collection

Details vary by plugin and source. To collect AWS CloudTrail logs, config can be as simple as:

```hcl
connection "aws" "prod" {
  profile = "SSO-Admin-605...13981"
}

partition "aws_cloudtrail_log" "prod" {
  source "aws_s3_bucket" {
    connection = connection.aws.prod
    bucket     = "aws-cloudtrail-logs-6054...81-fe67"
  }
}
```

## Run a collection

```
tailpipe collect aws_cloudtrail_log
```

This command will:

- Acquire compressed (.gz) log files

- Uncompress them

- Parse all the .json log files and map fields of each line to the plugin-defined schema

- Store the data in Parquet organized by date

## Query!

List the top 10 events and how many times they were called.

```sql
tailpipe query
>  select
  event_source,
  event_name,
  count(*) as event_count
from
  aws_cloudtrail_log
group by
  event_source,
  event_name,
order by
  event_count desc
limit 10;
```

```
+-------------------+---------------------------+-------------+
| event_source      | event_name                | event_count |
+-------------------+---------------------------+-------------+
| ec2.amazonaws.com | RunInstances              | 1225268     |
| ec2.amazonaws.com | DescribeSnapshots         | 101158      |
| sts.amazonaws.com | AssumeRole                | 78380       |
| s3.amazonaws.com  | GetBucketAcl              | 19095       |
| ec2.amazonaws.com | DescribeInstances         | 18366       |
| sts.amazonaws.com | GetCallerIdentity         | 16512       |
| iam.amazonaws.com | GetPolicyVersion          | 14737       |
| s3.amazonaws.com  | ListBuckets               | 13206       |
| ec2.amazonaws.com | DescribeSpotPriceHistory  | 10714       |
| ec2.amazonaws.com | DescribeSnapshotAttribute | 9107        |
+-------------------+---------------------------+-------------+
```

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

[Tailpipe](https://tailpipe-io.vercel.app) is a product produced from this open source software, exclusively by [Turbot HQ, Inc](https://turbot.com). It is distributed under our commercial terms. Others are allowed to make their own distribution of the software, but cannot use any of the Turbot trademarks, cloud services, etc. You can learn more in our [Open Source FAQ](https://turbot.com/open-source).

## Get involved

**[Join #tailpipe on Slack →](https://turbot.com/community/join)**


