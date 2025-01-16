load "$LIB_BATS_ASSERT/load.bash"
load "$LIB_BATS_SUPPORT/load.bash"

@test "verify --from works in tailpipe query" {
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/chaos_date_time.tpc
partition "chaos_date_time" "date_time_inc" {
  source "chaos_date_time" {
    row_count = 100
  }
}
EOF

  # tailpipe collect
  tailpipe collect chaos_date_time.date_time_inc

  # run tailpipe query with --from and verify the timestamps
  run tailpipe query "select tp_timestamp from chaos_date_time order by tp_timestamp asc" --output csv --from="2007-01-25"
  echo $output

  # output should only contain dates from 2007-01-25 and later
  assert_equal "$output" "tp_timestamp
2007-01-25 15:04:05
2007-01-29 15:04:05
2007-02-02 15:04:05"

  # remove the config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/chaos_date_time.tpc
}

@test "verify --from works when ISO 8601 datetime is passed" {
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/chaos_date_time.tpc
partition "chaos_date_time" "date_time_inc" {
  source "chaos_date_time" {
    row_count = 100
  }
}
EOF

  # tailpipe collect
  tailpipe collect chaos_date_time.date_time_inc

  # run tailpipe query with --from and verify the timestamps
  run tailpipe query "select tp_timestamp from chaos_date_time order by tp_timestamp asc" --output csv --from="2007-01-25T15:04:05"
  echo $output

  # output should only contain dates from 2007-01-25T15:04:05 and later
  assert_equal "$output" "tp_timestamp
2007-01-25 15:04:05
2007-01-29 15:04:05
2007-02-02 15:04:05"

  # remove the config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/chaos_date_time.tpc
}

@test "verify --from works when ISO 8601 datetime with milliseconds is passed" {
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/chaos_date_time.tpc
partition "chaos_date_time" "date_time_inc" {
  source "chaos_date_time" {
    row_count = 100
  }
}
EOF

  # tailpipe collect
  tailpipe collect chaos_date_time.date_time_inc

  # run tailpipe query with --from and verify the timestamps
  run tailpipe query "select tp_timestamp from chaos_date_time order by tp_timestamp asc" --output csv --from="2007-01-25T15:04:05.000"
  echo $output

  # output should only contain dates from 2007-01-25T15:04:05.000 and later
  assert_equal "$output" "tp_timestamp
2007-01-25 15:04:05
2007-01-29 15:04:05
2007-02-02 15:04:05"

  # remove the config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/chaos_date_time.tpc
}

@test "verify --from works when RFC 3339 datetime with timezone is passed" {
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/chaos_date_time.tpc
partition "chaos_date_time" "date_time_inc" {
  source "chaos_date_time" {
    row_count = 100
  }
}
EOF

  # tailpipe collect
  tailpipe collect chaos_date_time.date_time_inc

  # run tailpipe query with --from and verify the timestamps
  run tailpipe query "select tp_timestamp from chaos_date_time order by tp_timestamp asc" --output csv --from="2007-01-25T15:04:05Z"
  echo $output

  # output should only contain dates from 2007-01-25T15:04:05Z and later
  assert_equal "$output" "tp_timestamp
2007-01-25 15:04:05
2007-01-29 15:04:05
2007-02-02 15:04:05"

  # remove the config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/chaos_date_time.tpc
}

@test "verify --from works when relative time(T-x) is passed" {
  skip "test this locally since testing relative times can be tricky"
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/chaos_date_time.tpc
partition "chaos_date_time" "date_time_inc" {
  source "chaos_date_time" {
    row_count = 100
  }
}
EOF

  # tailpipe collect
  tailpipe collect chaos_date_time.date_time_inc

  # run tailpipe query with --from and verify the timestamps
  run tailpipe query "select tp_timestamp from chaos_date_time order by tp_timestamp asc" --output csv --from="T-18Y"
  echo $output

  # output should only contain dates from T-18Y and later
  assert_equal "$output" "tp_timestamp
2007-01-13 15:04:05
2007-01-17 15:04:05
2007-01-21 15:04:05
2007-01-25 15:04:05
2007-01-29 15:04:05
2007-02-02 15:04:05"

  # remove the config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/chaos_date_time.tpc
}


function teardown() {
  rm -rf $TAILPIPE_INSTALL_DIR/data
}
