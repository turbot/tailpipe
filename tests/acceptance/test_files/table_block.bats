load "$LIB_BATS_ASSERT/load.bash"
load "$LIB_BATS_SUPPORT/load.bash"

@test "verify column transforms" {
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/format_delimited.tpc
format "delimited" "transform_test" {
  delimiter = ","
}
EOF

  cat << EOF > $TAILPIPE_INSTALL_DIR/config/table_transform.tpc
table "transform_test" {
  format = format.delimited.transform_test

  column "tp_timestamp" {
    source = "timestamp"
    type = "datetime"
  }

  column "tp_date" {
    source = "timestamp"
    type = "date"
  }

  column "value_doubled" {
    type = "integer"
    transform = "raw_value * 2"
  }

  column "status_category" {
    type = "varchar"
    transform = "CASE WHEN status_code < 300 THEN 'success' WHEN status_code < 400 THEN 'redirect' WHEN status_code < 500 THEN 'client_error' ELSE 'server_error' END"
  }

  column "browser" {
    type = "varchar"
    transform = "CASE WHEN user_agent LIKE '%Windows%' THEN 'Windows' WHEN user_agent LIKE '%Macintosh%' THEN 'Mac' ELSE 'Other' END"
  }

  column "is_internal" {
    type = "boolean"
    transform = "ip_address LIKE '192.168.%' OR ip_address LIKE '10.%' OR ip_address LIKE '172.16.%'"
  }

  column "parsed_time" {
    type = "datetime"
    transform = "strptime(CAST(custom_time AS VARCHAR), '%Y-%m-%d %H:%M:%S')"
  }
}

partition "transform_test" "local" {
  source "file" {
    format = format.delimited.transform_test
    paths = ["$SOURCE_FILES_DIR/custom_logs/"]
    file_layout = "transform_data.csv"
  }
}
EOF

  # Run collection and verify
  tailpipe collect transform_test --progress=false --from=2024-05-01

  # Verify transformations
  run tailpipe query "select value_doubled, status_category, browser, is_internal, parsed_time from transform_test order by tp_timestamp" --output csv
  echo $output

  assert_equal "$output" "value_doubled,status_category,browser,is_internal,parsed_time
84,success,Windows,true,2024-05-01 10:00:00
198,client_error,Mac,true,2024-05-01 10:01:00
300,server_error,Other,true,2024-05-01 10:02:00"

  # Cleanup
  rm -rf $TAILPIPE_INSTALL_DIR/config/format_delimited.tpc
  rm -rf $TAILPIPE_INSTALL_DIR/config/table_transform.tpc
}

@test "verify null_if in column blocks" {
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/format_delimited_null.tpc
format "delimited" "null_if_test" {
  delimiter = ","
}
EOF

  cat << EOF > $TAILPIPE_INSTALL_DIR/config/table_null_if.tpc
table "null_if_test" {
  format = format.delimited.null_if_test

  column "tp_timestamp" {
    type = "datetime"
    transform = "now()"
  }

  column "tp_date" {
    type = "date"
    transform = "current_date"
  }

  column "id" {
    type = "integer"
    source = "id"
  }

  column "status" {
    type = "varchar"
    source = "status"
    null_if = "inactive"
  }

  column "value" {
    type = "integer"
    source = "value"
    null_if = "0"
  }

  column "description" {
    type = "varchar"
    source = "description"
  }
}

partition "null_if_test" "local" {
  source "file" {
    format = format.delimited.null_if_test
    paths = ["$SOURCE_FILES_DIR/custom_logs/"]
    file_layout = "null_if_data.csv"
  }
}
EOF

  # Run collection and verify
  tailpipe collect null_if_test --progress=false --from=2024-05-01

  # Verify null_if transformations
  run tailpipe query "select id, status, value, description from null_if_test order by id" --output csv
  echo $output

  assert_equal "$output" "id,status,value,description
1,active,42,normal value
2,<null>,<null>,zero value
3,active,-1,negative value
4,active,2,empty value
5,active,999,special value"

  # Cleanup
  rm -rf $TAILPIPE_INSTALL_DIR/config/format_delimited_null.tpc
  rm -rf $TAILPIPE_INSTALL_DIR/config/table_null_if.tpc
}

function teardown() {
  rm -rf $TAILPIPE_INSTALL_DIR/data
} 