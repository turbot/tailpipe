load "$LIB_BATS_ASSERT/load.bash"
load "$LIB_BATS_SUPPORT/load.bash"

@test "verify grok format definition" {
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/format_grok.tpc
format "grok" "steampipe_plugin" {
  layout = \`%{TIMESTAMP_ISO8601:timestamp} %{WORD:timezone} \[%{LOGLEVEL:severity}\]\s+(?:%{NOTSPACE:plugin_name}: \[%{LOGLEVEL:plugin_severity}\]\s+%{NUMBER:plugin_timestamp}:\s+)?%{GREEDYDATA:message}\`
}

table "steampipe_plugin" {
  format = format.grok.steampipe_plugin

  column "tp_timestamp" {
    source = "timestamp"
  }

  column "plugin_timestamp" {
    type = "timestamp"
  }
}

partition "steampipe_plugin" "local" {
  source "file" {
    format = format.grok.steampipe_plugin
    paths = ["$SOURCE_FILES_DIR/custom_logs/"]
    file_layout = \`plugin-%{YEAR:year}-%{MONTHNUM:month}-%{MONTHDAY:day}.log\`
  }
}
EOF

  # Run collection and verify
  tailpipe collect steampipe_plugin --progress=false --from=2025-04-26

  # Verify data was collected correctly
  run tailpipe query "select plugin_name, tp_timestamp from steampipe_plugin limit 1" --output csv
  echo $output

  assert_equal "$output" "plugin_name,tp_timestamp
steampipe-plugin-aws.plugin,2025-04-28 15:16:35"

  # Cleanup
  rm -rf $TAILPIPE_INSTALL_DIR/config/format_grok.tpc
}

@test "verify delimited format definition" {
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/format_delimited.tpc
format "delimited" "access_log" {
  delimiter = ","
  header = true
}

table "access_log" {
  format = format.delimited.access_log

  column "tp_timestamp" {
    source = "timestamp"
  }

  column "ip_address" {
    type = "varchar"
  }

  column "user_agent" {
    type = "varchar"
  }

  column "status_code" {
    type = "integer"
  }
}

partition "access_log" "local" {
  source "file" {
    format = format.delimited.access_log
    paths = ["$SOURCE_FILES_DIR/custom_logs/"]
    file_layout = "access_log.csv"
  }
}
EOF

  # Run collection and verify
  tailpipe collect access_log --progress=false --from=2024-04-30

  # Verify data was collected correctly
  run tailpipe query "select ip_address, status_code from access_log limit 1" --output csv
  echo $output

  assert_equal "$output" "ip_address,status_code
192.168.1.1,200"

  # Cleanup
  rm -rf $TAILPIPE_INSTALL_DIR/config/format_delimited.tpc
}

@test "verify jsonl format definition" {
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/format_jsonl.tpc
format "jsonl" "server_metrics" {
  description = "Server metrics in JSON Lines format"
}

table "server_metrics" {
  format = format.jsonl.server_metrics

  column "tp_timestamp" {
    source = "timestamp"
  }

  column "server_id" {
    type = "varchar"
  }

  column "cpu_usage" {
    type = "float"
  }

  column "memory_used" {
    type = "integer"
  }

  column "is_healthy" {
    type = "boolean"
  }
}

partition "server_metrics" "local" {
  source "file" {
    format = format.jsonl.server_metrics
    paths = ["$SOURCE_FILES_DIR/custom_logs/"]
    file_layout = "server_metrics.jsonl"
  }
}
EOF

  # Run collection and verify
  tailpipe collect server_metrics --progress=false --from=2024-04-30

  # Verify data was collected correctly
  run tailpipe query "select server_id, cpu_usage, memory_used, is_healthy from server_metrics limit 1" --output csv
  echo $output

  assert_equal "$output" "server_id,cpu_usage,memory_used,is_healthy
srv-001,75.5,\"8,192\",true"

  # Cleanup
  rm -rf $TAILPIPE_INSTALL_DIR/config/format_jsonl.tpc
}

@test "verify regex format definition" {
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/format_regex.tpc
format "regex" "plugin_log" {
  layout = \`^(?P<timestamp>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d{3})\s+(?P<timezone>\w+)\s+\[(?P<log_level>\w+)\]\s+(?P<plugin_name>[\w.-]+)?(?:\s*:\s*\[(?P<plugin_log_level>\w+)\]\s+(?P<plugin_timestamp>\d+):\s+)?(?P<message>.*)\`
}

table "plugin_log" {
  format = format.regex.plugin_log

  column "tp_timestamp" {
    source = "timestamp"
  }

  column "log_level" {
    type = "varchar"
  }

  column "plugin_name" {
    type = "varchar"
  }

  column "plugin_log_level" {
    type = "varchar"
  }

  column "message" {
    type = "varchar"
  }
}

partition "plugin_log" "local" {
  source "file" {
    format = format.regex.plugin_log
    paths = ["$SOURCE_FILES_DIR/custom_logs/"]
    file_layout = \`plugin-%{YEAR:year}-%{MONTHNUM:month}-%{MONTHDAY:day}.log\`
  }
}
EOF

  # Run collection and verify
  tailpipe collect plugin_log --progress=false --from=2025-04-28

  # Verify data was collected correctly
  run tailpipe query "select log_level, plugin_name, message from plugin_log limit 1" --output csv
  echo $output

  assert_equal "$output" "log_level,plugin_name,message
DEBUG,steampipe-plugin-aws.plugin,\"retrying request Lambda/ListFunctions, attempt 8\""

  # Cleanup
  rm -rf $TAILPIPE_INSTALL_DIR/config/format_regex.tpc
}

@test "verify grok format with nested patterns" {
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/format_grok_nested.tpc
format "grok" "aws_log" {
  layout = \`%{TIMESTAMP_ISO8601:timestamp} \[%{LOGLEVEL:log_level}\] \[AWS\] RequestID: %{NOTSPACE:request_id}, Service: %{WORD:service}, Operation: %{WORD:operation}(?:, Duration: %{NUMBER:duration}ms)?(?:, Error: %{GREEDYDATA:error})?\`
}

table "aws_log" {
  format = format.grok.aws_log

  column "tp_timestamp" {
    source = "timestamp"
  }

  column "log_level" {
    type = "varchar"
  }

  column "request_id" {
    type = "varchar"
  }

  column "service" {
    type = "varchar"
  }

  column "operation" {
    type = "varchar"
  }

  column "duration" {
    type = "integer"
  }

  column "error" {
    type = "varchar"
  }
}

partition "aws_log" "local" {
  source "file" {
    format = format.grok.aws_log
    paths = ["$SOURCE_FILES_DIR/custom_logs/"]
    file_layout = "nested_patterns.log"
  }
}
EOF

  # Run collection and verify
  tailpipe collect aws_log --progress=false --from=2024-04-30

  # Verify data was collected correctly
  run tailpipe query "select log_level, service, operation, duration from aws_log order by request_id" --output csv
  echo $output

  assert_equal "$output" "log_level,service,operation,duration
INFO,s3,ListBuckets,150
ERROR,ec2,DescribeInstances,
DEBUG,lambda,Invoke,45"

  # Cleanup
  rm -rf $TAILPIPE_INSTALL_DIR/config/format_grok_nested.tpc
}

function teardown() {
  rm -rf $TAILPIPE_INSTALL_DIR/data
} 