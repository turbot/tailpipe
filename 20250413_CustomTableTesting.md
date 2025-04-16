# 20250413 Custom Table Testing

## Row Enrichment

### Basic Test - Custom Regex Format [Core Plugin Only]

Basic minimal test

Config
```hcl
table "t" {
  column "tp_timestamp" {
    source = "time_local"
  }
}

format "regex" "r" {
  layout = `^(?P<remote_addr>[^ ]*) - (?P<remote_user>[^ ]*) \[(?P<time_local>[^\]]*)\] "(?P<request_method>\S+)(?: +(?P<request_uri>[^ ]+))?(?: +(?P<server_protocol>\S+))?" (?P<status>[^ ]*) (?P<body_bytes_sent>[^ ]*) "(?P<http_referer>.*?)" "(?P<http_user_agent>.*?)"`
}

partition "t" "p" {
  source "file"  {
    format      = format.regex.r
    paths       = ["/Users/graza/tailpipe_data/nginx_access_logs"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

Query
```
> select tp_timestamp, body_bytes_sent, http_referer, http_user_agent, remote_addr, remote_user, request_method, request_uri, server_protocol, status, time_local from t limit 5;
+---------------------+-----------------+--------------+--------------------------------------------------------------------------------------------------------------------------------------------+-----------------+-------------+----------------+-----------------------------------------------------------------+-----------------+--------+----------------------------+
| tp_timestamp        | body_bytes_sent | http_referer | http_user_agent                                                                                                                            | remote_addr     | remote_user | request_method | request_uri                                                     | server_protocol | status | time_local                 |
+---------------------+-----------------+--------------+--------------------------------------------------------------------------------------------------------------------------------------------+-----------------+-------------+----------------+-----------------------------------------------------------------+-----------------+--------+----------------------------+
| 2024-07-29 10:30:20 | 3041            | -            | Opera/9.57 (Macintosh; U; Intel Mac OS X 10_8_5; en-US) Presto/2.8.308 Version/10.00                                                       | 4.73.13.210     | -           | DELETE         | /capability/Pre-emptive/Distributed/hybrid/actuating.css        | HTTP/1.1        | 200    | 29/Jul/2024:11:30:20 +0100 |
| 2024-07-29 10:30:21 | 2764            | -            | Mozilla/5.0 (iPad; CPU OS 9_2_2 like Mac OS X; en-US) AppleWebKit/536.18.6 (KHTML, like Gecko) Version/3.0.5 Mobile/8B112 Safari/6536.18.6 | 120.201.143.102 | -           | GET            | /exuding%20Horizontal-paradigm-discrete_parallelism.htm         | HTTP/1.1        | 200    | 29/Jul/2024:11:30:21 +0100 |
| 2024-07-29 10:30:22 | 45              | -            | Mozilla/5.0 (Windows NT 5.01; en-US; rv:1.9.0.20) Gecko/1933-06-04 Firefox/35.0                                                            | 181.214.126.176 | -           | PUT            | /client-server/Persevering/hybrid-analyzing/standardization.css | HTTP/1.1        | 404    | 29/Jul/2024:11:30:22 +0100 |
| 2024-07-29 10:30:23 | 1176            | -            | Opera/9.60 (X11; Linux x86_64; en-US) Presto/2.8.280 Version/11.00                                                                         | 42.72.2.165     | -           | PATCH          | /Automated/asynchronous/knowledge%20base.js                     | HTTP/1.1        | 200    | 29/Jul/2024:11:30:23 +0100 |
| 2024-07-29 10:30:24 | 1035            | -            | Mozilla/5.0 (X11; Linux x86_64; rv:6.0) Gecko/1913-11-04 Firefox/36.0                                                                      | 80.164.243.11   | -           | GET            | /next%20generation-knowledge%20user/encryption_Sharable.js      | HTTP/1.1        | 200    | 29/Jul/2024:11:30:24 +0100 |
+---------------------+-----------------+--------------+--------------------------------------------------------------------------------------------------------------------------------------------+-----------------+-------------+----------------+-----------------------------------------------------------------+-----------------+--------+----------------------------+
```

### Basic Test - Nginx Preset Format [Core/Nginx Cross Plugin]

Config
```hcl
table "t" {
  column "tp_timestamp" {
    source   = "time_local"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.default
    paths       = ["/Users/graza/tailpipe_data/nginx_access_logs"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

Query
```
> select tp_timestamp, body_bytes_sent, http_referer, http_user_agent, remote_addr, remote_user, request_method, request_uri, server_protocol, status, time_local from t limit 5;
+---------------------+-----------------+--------------+-----------------------------------------------------------------------------------------------------------------+-----------------+-------------+----------------+---------------------------------------------------------+-----------------+--------+----------------------------+
| tp_timestamp        | body_bytes_sent | http_referer | http_user_agent                                                                                                 | remote_addr     | remote_user | request_method | request_uri                                             | server_protocol | status | time_local                 |
+---------------------+-----------------+--------------+-----------------------------------------------------------------------------------------------------------------+-----------------+-------------+----------------+---------------------------------------------------------+-----------------+--------+----------------------------+
| 2024-07-29 10:05:41 | 43              | -            | Mozilla/5.0 (Windows 98; Win 9x 4.90) AppleWebKit/5310 (KHTML, like Gecko) Chrome/38.0.801.0 Mobile Safari/5310 | 141.25.76.110   | -           | GET            | /array/core%20knowledge%20user/Cross-group.gif          | HTTP/1.1        | 301    | 29/Jul/2024:11:05:41 +0100 |
| 2024-07-29 10:05:42 | 1401            | -            | Mozilla/5.0 (Windows NT 5.0) AppleWebKit/5331 (KHTML, like Gecko) Chrome/36.0.868.0 Mobile Safari/5331          | 69.181.196.125  | -           | GET            | /structure-Managed%20project/middleware-application.htm | HTTP/1.1        | 200    | 29/Jul/2024:11:05:42 +0100 |
| 2024-07-29 10:05:43 | 74              | -            | Mozilla/5.0 (Windows NT 5.01; en-US; rv:1.9.1.20) Gecko/1946-10-03 Firefox/37.0                                 | 50.247.11.252   | -           | DELETE         | /flexibility-Persevering/mission-critical/Monitored.js  | HTTP/1.1        | 400    | 29/Jul/2024:11:05:43 +0100 |
| 2024-07-29 10:05:44 | 2877            | -            | Mozilla/5.0 (X11; Linux i686; rv:7.0) Gecko/1959-02-09 Firefox/36.0                                             | 101.223.247.121 | -           | POST           | /grid-enabled/capacity-Persevering/radical.hmtl         | HTTP/1.1        | 200    | 29/Jul/2024:11:05:44 +0100 |
| 2024-07-29 10:05:45 | 2563            | -            | Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_6_10 rv:2.0) Gecko/1924-25-04 Firefox/36.0                           | 168.10.143.27   | -           | GET            | /Persevering%20Open-architected_national/Horizontal.js  | HTTP/1.1        | 200    | 29/Jul/2024:11:05:45 +0100 |
+---------------------+-----------------+--------------+-----------------------------------------------------------------------------------------------------------------+-----------------+-------------+----------------+---------------------------------------------------------+-----------------+--------+----------------------------+
```

### Basic Test - Nginx Custom Format [Core/Nginx Cross Plugin]

```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  column "tp_timestamp" {
    source   = "time_local"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

Query
```
> SELECT tp_timestamp, body_bytes_sent, http_referer, http_user_agent, remote_addr, remote_user, request_method, request_uri, server_protocol, status, time_local, request_time FROM t LIMIT 5;
+---------------------+-----------------+--------------+---------------------------------------------------------------------------------------------------------------------------+-----------------+-------------+----------------+-----------------+-----------------+--------+----------------------------+--------------+
| tp_timestamp        | body_bytes_sent | http_referer | http_user_agent                                                                                                           | remote_addr     | remote_user | request_method | request_uri     | server_protocol | status | time_local                 | request_time |
+---------------------+-----------------+--------------+---------------------------------------------------------------------------------------------------------------------------+-----------------+-------------+----------------+-----------------+-----------------+--------+----------------------------+--------------+
| 2025-03-30 09:37:55 | 915756          | -            | Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 | 174.132.116.92  | -           | GET            | /api/v1/payment | HTTP/1.1        | 204    | 30/Mar/2025:10:37:55 +0100 | 0.327        |
| 2025-03-30 09:37:56 | 845190          | -            | Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36       | 243.52.146.253  | -           | GET            | /wp-admin       | HTTP/1.1        | 301    | 30/Mar/2025:10:37:56 +0100 | 3.906        |
| 2025-03-30 09:37:56 | 469267          | -            | Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 | 121.55.217.233  | -           | DELETE         | /metrics        | HTTP/1.1        | 204    | 30/Mar/2025:10:37:56 +0100 | 0.328        |
| 2025-03-30 09:37:57 | 174142          | -            | Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 | 192.168.25.127  | -           | GET            | /               | HTTP/1.1        | 200    | 30/Mar/2025:10:37:57 +0100 | 0.241        |
| 2025-03-30 09:37:58 | 465818          | -            | Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 | 102.201.100.205 | -           | DELETE         | /api/v1/orders  | HTTP/1.1        | 301    | 30/Mar/2025:10:37:58 +0100 | 5.592        |
```

### Custom Table - Add Description

Config
```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  description = "nginx access log"
  
  column "tp_timestamp" {
    source   = "time_local"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

```sh
❯ tailpipe table list
NAME                 PLUGIN                                          LOCAL SIZE    FILES    ROWS       DESCRIPTION
apache_access_log    hub.tailpipe.io/plugins/turbot/apache@latest    -             -        -
nginx_access_log     hub.tailpipe.io/plugins/turbot/nginx@latest     -             -        -
t                    hub.tailpipe.io/plugins/turbot/core@latest      7.5 MB        2        186,297    nginx access log
```

### NuffIf - Table Level

Note: If all values are `null` the column isn't in the output dataset, for example setting to `-` removes the `http_referer` and `remote_user` columns

Config
```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  description = "nginx access log"

  null_if = "-"

  column "tp_timestamp" {
    source   = "time_local"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

```
> .inspect t
Column    
          Type
body_bytes_sent     varchar
http_user_agent     varchar
remote_addr         varchar
request_method      varchar
request_time        varchar
request_uri         varchar
server_protocol     varchar
status              varchar
time_local          varchar
tp_
```

If you set it to `200` you can null the `status` rows which have a success code:
Config
```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  description = "nginx access log"

  null_if = "200"

  column "tp_timestamp" {
    source   = "time_local"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```
```
> select distinct status from t;
+--------+
| status |
+--------+
| 401    |
| <null> |
| 404    |
| 500    |
| 201    |
| 204    |
| 302    |
| 403    |
| 400    |
| 301    |
+--------+
```

### NuffIf - Column Level

Removing 404 status from our rows

Config:
```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  description = "nginx access log"

  column "tp_timestamp" {
    source   = "time_local"
  }

  column "status" {
    source   = "status"
    null_if  = "404"
    type     = "integer"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

```
> select distinct status from t;
+--------+
| status |
+--------+
| 200    |
| <null> |
| 301    |
| 401    |
| 302    |
| 500    |
| 400    |
| 403    |
| 204    |
| 201    |
+--------+
```

### Transform - Amend existing column

Make request_method lowercase

Config:
```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  column "tp_timestamp" {
    source   = "time_local"
  }

  column "request_method" {
    transform = "lower(request_method)"
    type      = "varchar"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

Query
```
> select distinct request_method from t;
+----------------+
| request_method |
+----------------+
| put            |
| post           |
| get            |
| delete         |
+----------------+
```

### Transform - Create a new column

Add `raw_request` the 3 components of $request (which we separate)

Config:
```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  column "tp_timestamp" {
    source   = "time_local"
  }

  column "raw_request" {
    transform = "concat(request_method, ' ', request_uri, ' ', server_protocol)"
    type      = "varchar"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

Query:
```
> select distinct raw_request from t limit 5;
+--------------------------------------------------------------------+
| raw_request                                                        |
+--------------------------------------------------------------------+
| PUT /api/v1/orders HTTP/1.1                                        |
| GET /api/v1/internal/config?..%2F..%2F..%2Fetc%2Fpasswd HTTP/1.1   |
| GET /health?..%2F..%2F..%2Fetc%2Fpasswd HTTP/1.1                   |
| PUT /api/v1/users?%3Cscript%3Ealert%281%29%3C%2Fscript%3E HTTP/1.1 |
| PUT /phpMyAdmin?%27+OR+%271%27%3D%271 HTTP/1.1                     |
+--------------------------------------------------------------------+
```

### Required Column Not Existing - Expect Errors

Config:
```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  column "tp_timestamp" {
    source   = "time_local"
  }

  column "not_here" {
    required = true
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

Output:
```sh
❯ tailpipe collect t.p --from T-10Y

Collecting logs for t.p from 2015-04-13

Artifacts:
  Discovered: 2
  Downloaded: 2 3.9MB
  Extracted:  2

Rows:
  Received: 186,297
  Enriched:       0
  Saved:          0
  Errors:   186,297

Files:
  No files to compact.

Errors:
  20250330_access.log.gz: 99,122 rows have missing fields: not_here
  20250331_access.log.gz: 87,175 rows have missing fields: not_here
  Set TAILPIPE_LOG_LEVEL=ERROR for details.

```

### MapFields - Obtain subset of data

Get only tp_fields (enforced), along with status and request* fields

Config:
```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  column "tp_timestamp" {
    source   = "time_local"
  }

  map_fields = ["status", "request*"]
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

```
> .inspect t
Column              Type
request_method      varchar
request_time        varchar
request_uri         varchar
status              varchar
tp_...
```

### MapFields - Given a field that doesn't exist

Since this is a pattern match, this should allow us to obtain tp_fields and no additional data

Config:
```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  column "tp_timestamp" {
    source   = "time_local"
  }

  map_fields = ["not_matching_pattern"]
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

```
> .inspect t
Column              Type
tp_akas             varchar[]
tp_date             date
tp_destination_ip   varchar
tp_domains          varchar[]
tp_emails           varchar[]
tp_id               varchar
tp_index            varchar
tp_ingest_timestamp timestamp
tp_ips              varchar[]
tp_partition        varchar
tp_source_ip        varchar
tp_source_location  varchar
tp_source_name      varchar
tp_source_type      varchar
tp_table            varchar
tp_tags             varchar[]
tp_timestamp        timestamp
tp_usernames        varchar[]
```

### MapFields - Wildcard Testing `*a*`

Config:
```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  column "tp_timestamp" {
    source   = "time_local"
  }

  map_fields = ["*a*"]
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

```
> .inspect t
Column              Type
http_user_agent     varchar
remote_addr         varchar
status              varchar
time_local          varchar
tp_...
```

### Transform - Non-Existent Function [Error Expected]

Config:
```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  column "tp_timestamp" {
    source   = "time_local"
  }

  column "request_method" {
    type      = "varchar"
    transform = "fake_function(request_method)"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

```
❯ tailpipe collect t.p --from T-10Y

Collecting logs for t.p from 2015-04-13

Artifacts:
  Discovered: 2
  Downloaded: 2 3.9MB
  Extracted:  2

Rows:
  Received: 186,297
  Enriched: 186,297
  Saved:          0
  Errors:   186,297

Files:
  No files to compact.

Errors:
  1744553252666-0.jsonl: Catalog Error: Scalar Function with name fake_function does not exist!
Did you mean "quantile_cont"?

LINE 4: mean "qufake_function(request_method) as "request_method",
        ^
  1744553252666-1.jsonl: Catalog Error: Scalar Function with name fake_function does not exist!
Did you mean "quantile_cont"?

LINE 4: mean "qufake_function(request_method) as "request_method",
        ^
  1744553252666-2.jsonl: Catalog Error: Scalar Function with name fake_function does not exist!
Did you mean "quantile_cont"?

LINE 4: mean "qufake_function(request_method) as "request_method",
        ^
  1744553252666-3.jsonl: Catalog Error: Scalar Function with name fake_function does not exist!
Did you mean "quantile_cont"?

LINE 4: mean "qufake_function(request_method) as "request_method",
        ^
  1744553252666-4.jsonl: Catalog Error: Scalar Function with name fake_function does not exist!
Did you mean "quantile_cont"?

LINE 4: mean "qufake_function(request_method) as "request_method",
        ^
  1744553252666-5.jsonl: Catalog Error: Scalar Function with name fake_function does not exist!
Did you mean "quantile_cont"?

LINE 4: mean "qufake_function(request_method) as "request_method",
        ^
  1744553252666-6.jsonl: Catalog Error: Scalar Function with name fake_function does not exist!
Did you mean "quantile_cont"?

LINE 4: mean "qufake_function(request_method) as "request_method",
        ^
  1744553252666-7.jsonl: Catalog Error: Scalar Function with name fake_function does not exist!
Did you mean "quantile_cont"?

LINE 4: mean "qufake_function(request_method) as "request_method",
        ^
  1744553252666-8.jsonl: Catalog Error: Scalar Function with name fake_function does not exist!
Did you mean "quantile_cont"?

LINE 4: mean "qufake_function(request_method) as "request_method",
        ^
  1744553252666-9.jsonl: Catalog Error: Scalar Function with name fake_function does not exist!
Did you mean "quantile_cont"?

LINE 4: mean "qufake_function(request_method) as "request_method",
        ^
  1744553252666-10.jsonl: Catalog Error: Scalar Function with name fake_function does not exist!
Did you mean "quantile_cont"?

LINE 4: mean "qufake_function(request_method) as "request_method",
        ^
  1744553252666-11.jsonl: Catalog Error: Scalar Function with name fake_function does not exist!
Did you mean "quantile_cont"?

LINE 4: mean "qufake_function(request_method) as "request_method",
        ^
  1744553252666-12.jsonl: Catalog Error: Scalar Function with name fake_function does not exist!
Did you mean "quantile_cont"?

LINE 4: mean "qufake_function(request_method) as "request_method",
        ^
  1744553252666-13.jsonl: Catalog Error: Scalar Function with name fake_function does not exist!
Did you mean "quantile_cont"?

LINE 4: mean "qufake_function(request_method) as "request_method",
        ^
  1744553252666-14.jsonl: Catalog Error: Scalar Function with name fake_function does not exist!
Did you mean "quantile_cont"?

LINE 4: 	fake_function(request_method) as "request_method",
        ^
  Truncated. Set TAILPIPE_LOG_LEVEL=ERROR for details.

Completed: 2s
```

### Transform - Same Field (essentially do nothing)

Config:
```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  column "tp_timestamp" {
    source   = "time_local"
  }

  column "request_method" {
    type      = "varchar"
    transform = "request_method"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

```
> select distinct request_method from t;
+----------------+
| request_method |
+----------------+
| PUT            |
| POST           |
| DELETE         |
| GET            |
+----------------+
```

### Transform - Replication of "Source"

Config:
```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  column "tp_timestamp" {
    source   = "time_local"
  }

  column "banana" {
    type      = "varchar"
    transform = "request_method"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

```
> select distinct banana from t;
+--------+
| banana |
+--------+
| PUT    |
| POST   |
| GET    |
| DELETE |
+--------+
```

### Transform - Replication of "Source" but with non-existent source

Config:
```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  column "tp_timestamp" {
    source   = "time_local"
  }

  column "banana" {
    type      = "varchar"
    transform = "not_here"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

```
❯ tailpipe collect t.p --from T-10Y

Collecting logs for t.p from 2015-04-13

Artifacts:
  Discovered: 2
  Downloaded: 2 3.9MB
  Extracted:  2

Rows:
  Received: 186,297
  Enriched: 186,297
  Saved:          0
  Errors:   186,297

Files:
  No files to compact.

Errors:
  1744553687330-0.jsonl: Binder Error: Referenced column "not_here" not found in FROM clause!
Candidate bindings: "tp_usernames", "http_user_agent", "remote_user", "remote_addr", "http_referer"

LINE 4: e bindinnot_here as "banana",
        ^
  1744553687330-1.jsonl: Binder Error: Referenced column "not_here" not found in FROM clause!
Candidate bindings: "tp_usernames", "http_user_agent", "remote_user", "remote_addr", "http_referer"

LINE 4: e bindinnot_here as "banana",
        ^
  1744553687330-2.jsonl: Binder Error: Referenced column "not_here" not found in FROM clause!
Candidate bindings: "tp_usernames", "http_user_agent", "remote_user", "remote_addr", "http_referer"

LINE 4: e bindinnot_here as "banana",
        ^
  1744553687330-3.jsonl: Binder Error: Referenced column "not_here" not found in FROM clause!
Candidate bindings: "tp_usernames", "http_user_agent", "remote_user", "remote_addr", "http_referer"

LINE 4: e bindinnot_here as "banana",
        ^
  1744553687330-4.jsonl: Binder Error: Referenced column "not_here" not found in FROM clause!
Candidate bindings: "tp_usernames", "http_user_agent", "remote_user", "remote_addr", "http_referer"

LINE 4: e bindinnot_here as "banana",
        ^
  1744553687330-5.jsonl: Binder Error: Referenced column "not_here" not found in FROM clause!
Candidate bindings: "tp_usernames", "http_user_agent", "remote_user", "remote_addr", "http_referer"

LINE 4: e bindinnot_here as "banana",
        ^
  1744553687330-6.jsonl: Binder Error: Referenced column "not_here" not found in FROM clause!
Candidate bindings: "tp_usernames", "http_user_agent", "remote_user", "remote_addr", "http_referer"

LINE 4: e bindinnot_here as "banana",
        ^
  1744553687330-7.jsonl: Binder Error: Referenced column "not_here" not found in FROM clause!
Candidate bindings: "tp_usernames", "http_user_agent", "remote_user", "remote_addr", "http_referer"

LINE 4: e bindinnot_here as "banana",
        ^
  1744553687330-8.jsonl: Binder Error: Referenced column "not_here" not found in FROM clause!
Candidate bindings: "tp_usernames", "http_user_agent", "remote_user", "remote_addr", "http_referer"

LINE 4: e bindinnot_here as "banana",
        ^
  1744553687330-9.jsonl: Binder Error: Referenced column "not_here" not found in FROM clause!
Candidate bindings: "tp_usernames", "http_user_agent", "remote_user", "remote_addr", "http_referer"

LINE 4: e bindinnot_here as "banana",
        ^
  1744553687330-10.jsonl: Binder Error: Referenced column "not_here" not found in FROM clause!
Candidate bindings: "tp_usernames", "http_user_agent", "remote_user", "remote_addr", "http_referer"

LINE 4: e bindinnot_here as "banana",
        ^
  1744553687330-11.jsonl: Binder Error: Referenced column "not_here" not found in FROM clause!
Candidate bindings: "tp_usernames", "http_user_agent", "remote_user", "remote_addr", "http_referer"

LINE 4: e bindinnot_here as "banana",
        ^
  1744553687330-12.jsonl: Binder Error: Referenced column "not_here" not found in FROM clause!
Candidate bindings: "tp_usernames", "http_user_agent", "remote_user", "remote_addr", "http_referer"

LINE 4: e bindinnot_here as "banana",
        ^
  1744553687330-13.jsonl: Binder Error: Referenced column "not_here" not found in FROM clause!
Candidate bindings: "tp_usernames", "http_user_agent", "remote_user", "remote_addr", "http_referer"

LINE 4: e bindinnot_here as "banana",
        ^
  1744553687330-14.jsonl: Binder Error: Referenced column "not_here" not found in FROM clause!
Candidate bindings: "tp_usernames", "http_user_agent", "remote_user", "remote_addr", "http_referer"

LINE 4: 	not_here as "banana",
        ^
  Truncated. Set TAILPIPE_LOG_LEVEL=ERROR for details.

Completed: 2s
```

### Transform - strptime (since we removed time_format)

Config:
```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  column "tp_timestamp" {
    source   = "time_local"
  }

  column "parsed_time" {
    type      = "timestamp"
    transform = "strptime(time_local, '%d/%b/%Y:%H:%M:%S %z')"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

```
> .inspect t
Column              Type
body_bytes_sent     varchar
http_referer        varchar
http_user_agent     varchar
parsed_time         timestamp with time zone
remote_addr         varchar
remote_user         varchar
request_method      varchar
request_time        varchar
request_uri         varchar
server_protocol     varchar
status              varchar
time_local          varchar
tp_...
```

> Additional Note: wasn't able to convert the type of time_local using this approach

```hcl
format "nginx_access_log" "custom" {
  layout = `$remote_addr - $remote_user [$time_local] "$request_method $request_uri $server_protocol" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time`
}

table "t" {
  column "tp_timestamp" {
    source   = "time_local"
  }

  column "time_local" {
    type      = "timestamp"
    transform = "strptime(time_local, '%d/%b/%Y:%H:%M:%S %z')"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.nginx_access_log.custom
    paths       = ["/Users/graza/tailpipe_data/nginx2"]
    file_layout = `%{YEAR:year}%{MONTHNUM:month}%{MONTHDAY:day}_%{DATA}.log`
  }
}
```

results in
```
  1744554292192-0.jsonl: Binder Error: No function matches the given name and argument types 'strptime(TIMESTAMP, STRING_LITERAL)'. You might need to add explicit type casts.
  Saved:Candidate functions:
  Errorsstrptime(VARCHAR, VARCHAR) -> TIMESTAMP
	strptime(VARCHAR, VARCHAR[]) -> TIMESTAMP


LINE 4: Candidatstrptime(time_local, '%d/%b/%Y:%H:%M:%S %z') as "time_local...
        ^
```

and omitting the type results in:
```
❯ tailpipe collect t.p --from T-10Y
Error: collection error: table 't' returned invalid schema: column type must be specified if column is optional (column 'time_local')
```


### Additional Notes / Actions
- Need to enhance/tidy error messages when function used in transform doesn't exist
- Need to enhance/tidy error messages when column used in transform doesn't exist
- Trying to convert a VARCHAR column to TIMESTAMP with a STRPTIME transform results in errors.

## JSONL Direct Conversion

### Basic Test

Minimal setup

Config:
```hcl
table "t" {
  column "tp_timestamp" {
    source = "timestamp"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.jsonl.default
    paths       = ["/Users/graza/tailpipe_data/jsonl"]
    file_layout = `.jsonl`
  }
}
```

```
> .inspect t
Column              Type
ip_address          varchar
method              varchar
path                varchar
protocol            varchar
query               varchar
request_headers     struct(accept varchar, "authorization" varchar, "user-agent" varchar)
request_id          varchar
request_time        double
response_headers    struct("cache-control" varchar, "content-type" varchar)
response_size       bigint
status              bigint
timestamp           varchar
tp_akas             varchar[]
tp_date             date
tp_destination_ip   varchar
tp_domains          varchar[]
tp_emails           varchar[]
tp_id               varchar
tp_index            varchar
tp_ingest_timestamp timestamp
tp_ips              varchar[]
tp_partition        varchar
tp_source_ip        varchar
tp_source_location  varchar
tp_source_name      varchar
tp_source_type      varchar
tp_table            varchar
tp_tags             varchar[]
tp_timestamp        timestamp
tp_usernames        varchar[]
user_agent          varchar
```

It looks like we've lost nested values!
```
+--------------------------------------------------------+
| request_headers                                        |
+--------------------------------------------------------+
| map[accept:<nil> authorization:<nil> user-agent:<nil>] |
| map[accept:<nil> authorization:<nil> user-agent:<nil>] |
| map[accept:<nil> authorization:<nil> user-agent:<nil>] |
| map[accept:<nil> authorization:<nil> user-agent:<nil>] |
| map[accept:<nil> authorization:<nil> user-agent:<nil>] |
| map[accept:<nil> authorization:<nil> user-agent:<nil>] |
| map[accept:<nil> authorization:<nil> user-agent:<nil>] |
| map[accept:<nil> authorization:<nil> user-agent:<nil>] |
| map[accept:<nil> authorization:<nil> user-agent:<nil>] |
| map[accept:<nil> authorization:<nil> user-agent:<nil>] |
+--------------------------------------------------------+
```

### Changing Struct to Json

Config:
```hcl
table "t" {
  column "tp_timestamp" {
    source = "timestamp"
  }

  column "request_headers" {
    type = "json"
  }

  column "response_headers" {
    type = "json"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.jsonl.default
    paths       = ["/Users/graza/tailpipe_data/jsonl"]
    file_layout = `.jsonl`
  }
}
```

Now we have our nested values back!
```
> select request_headers from t limit 10;
+----------------------------------------------------------------------------------------------------------------------+
| request_headers                                                                                                      |
+----------------------------------------------------------------------------------------------------------------------+
| map[Accept:application/json Authorization:Bearer token123 User-Agent:okhttp/4.9.0]                                   |
| map[Accept:application/json Authorization:Bearer token123 User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0_1)] |
| map[Accept:application/json Authorization:Bearer token123 User-Agent:curl/8.0.1]                                     |
| map[Accept:application/json Authorization:Bearer token123 User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0_1)] |
| map[Accept:application/json Authorization:Bearer token123 User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0_1)] |
| map[Accept:application/json Authorization:Bearer token123 User-Agent:Mozilla/5.0 (Windows NT 10.0; Win64; x64)]      |
| map[Accept:application/json Authorization:Bearer token123 User-Agent:okhttp/4.9.0]                                   |
| map[Accept:application/json Authorization:Bearer token123 User-Agent:PostmanRuntime/7.32.0]                          |
| map[Accept:application/json Authorization:Bearer token123 User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0_1)] |
| map[Accept:application/json Authorization:Bearer token123 User-Agent:PostmanRuntime/7.32.0]                          |
+----------------------------------------------------------------------------------------------------------------------+
```

### Complex Test - MapFields/Transforms/Etc

Config:
```hcl
table "t" {
  column "tp_timestamp" {
    source = "timestamp"
  }

  column "request_headers" {
    type = "json"
  }

  column "response_headers" {
    type = "json"
  }

  column "client" {
    source = "ip_address"
    type   = "varchar"
  }

  column "summary" {
    transform = "client || ' attemped to ' || lower(method) || ' ' || path || coalesce(query,'')"
    type      = "varchar"
  }

  map_fields = [
    "status"
  ]
}

partition "t" "p" {
  source "file"  {
    format      = format.jsonl.default
    paths       = ["/Users/graza/tailpipe_data/jsonl"]
    file_layout = `.jsonl`
  }
}
```

```
> .inspect t
Column              Type
client              varchar
request_headers     json
response_headers    json
status              bigint
summary             varchar
tp_akas             varchar[]
tp_date             date
tp_destination_ip   varchar
tp_domains          varchar[]
tp_emails           varchar[]
tp_id               varchar
tp_index            varchar
tp_ingest_timestamp timestamp
tp_ips              varchar[]
tp_partition        varchar
tp_source_ip        varchar
tp_source_location  varchar
tp_source_name      varchar
tp_source_type      varchar
tp_table            varchar
tp_tags             varchar[]
tp_timestamp        timestamp
tp_usernames        varchar[]
```

```
> select summary from t limit 10;
+--------------------------------------------------+
| summary                                          |
+--------------------------------------------------+
| 119.83.160.27 attemped to delete /api/orders     |
| 98.59.183.181 attemped to put /dashboard         |
| 113.136.102.108 attemped to delete /dashboard    |
| 232.56.198.126 attemped to get /dashboard?page=1 |
| 173.209.26.227 attemped to put /api/orders       |
| 218.32.250.216 attemped to get /logout           |
| 226.71.3.13 attemped to get /logout?sort=asc     |
| 68.1.173.68 attemped to delete /login            |
| 128.10.66.112 attemped to delete /api/orders     |
| 193.25.149.73 attemped to get /dashboard         |
+--------------------------------------------------+
```

### Additional Notes / Actions
- ERROR: Nested when automatically converted to struct fields have now been lower-cased and lost their values
  - Can alleviate this by setting column to `json` type and then we keep the casing/values of nested objects

## Delimited Direct Conversion

### Basic Test

Config:
```hcl
table "t" {
  column "tp_timestamp" {
    source = "BillingPeriodStartDate"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.delimited.default
    paths       = ["/Users/graza/tailpipe_data/billing"]
    file_layout = `.csv`
  }
}
```

Sh:
```sh
❯ tailpipe collect t.p --from T-10Y

Collecting logs for t.p from 2015-04-13

Artifacts:
  Discovered: 35
  Downloaded: 35 223MB
  Extracted:  35

Rows:
  Received: 474,534
  Enriched: 474,534
  Saved:    449,590
  Errors:    24,944

Files:
  Compacted: 33 => 33

Errors:
  174455638275-6.jsonl: inferred schema change detected - consider specifying a column type in table definition: 'InvoiceID': 'bigint' -> 'varchar'
  174455638275-34.jsonl: inferred schema change detected - consider specifying a column type in table definition: 'InvoiceID': 'bigint' -> 'varchar'
  Set TAILPIPE_LOG_LEVEL=ERROR for details.

Completed: 4s
```

Query:
```
> select count(*) from t;
+--------------+
| count_star() |
+--------------+
| 449590       |
+--------------+
> .inspect t
Column                 Type
BillingPeriodEndDate   timestamp
BillingPeriodStartDate timestamp
BlendedRate            json
CostBeforeTax          double
Credits                double
CurrencyCode           varchar
InvoiceDate            timestamp
InvoiceID              bigint
ItemDescription        varchar
LinkedAccountId        varchar
LinkedAccountName      varchar
Operation              varchar
PayerAccountId         bigint
PayerAccountName       varchar
PayerPONumber          json
ProductCode            varchar
ProductName            varchar
RateId                 bigint
RecordID               varchar
RecordType             varchar
SellerOfRecord         varchar
TaxAmount              double
TaxType                varchar
TaxationAddress        json
TotalCost              double
UsageEndDate           timestamp
UsageQuantity          double
UsageStartDate         timestamp
UsageType              varchar
tp_akas                varchar[]
tp_date                date
tp_destination_ip      varchar
tp_domains             varchar[]
tp_emails              varchar[]
tp_id                  varchar
tp_index               varchar
tp_ingest_timestamp    timestamp
tp_ips                 varchar[]
tp_partition           varchar
tp_source_ip           varchar
tp_source_location     varchar
tp_source_name         varchar
tp_source_type         varchar
tp_table               varchar
tp_tags                varchar[]
tp_timestamp           timestamp
tp_usernames           varchar[]
```

### Changing Type

InvoiceID can be omitted, or non-numeric so adjusted this to varchar

Config:
```hcl
table "t" {
  column "tp_timestamp" {
    source = "BillingPeriodStartDate"
  }

  column "InvoiceID" {
    type = "varchar"
  }
}

partition "t" "p" {
  source "file"  {
    format      = format.delimited.default
    paths       = ["/Users/graza/tailpipe_data/billing"]
    file_layout = `.csv`
  }
}
```

```
❯ tailpipe collect t.p --from T-10Y

Collecting logs for t.p from 2015-04-13

Artifacts:
  Discovered: 35
  Downloaded: 35 223MB
  Extracted:  35

Rows:
  Received: 474,534
  Enriched: 474,534
  Saved:    474,533
  Errors:         1

Files:
  Compacted: 35 => 35

Errors:
  1744556892361-34.jsonl: validation failed - found null values in columns: tp_timestamp, tp_date
  Set TAILPIPE_LOG_LEVEL=ERROR for details.

Completed: 4s
```

