load "$LIB_BATS_ASSERT/load.bash"
load "$LIB_BATS_SUPPORT/load.bash"

@test "verify partition with specific row count" {
  # Create a test partition configuration
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/chaos_all_columns.tpc
partition "chaos_all_columns" "row_count_test" {
  source "chaos_all_columns" {
    row_count = 5
  }
}
EOF

  # Run tailpipe collect
  tailpipe collect chaos_all_columns.row_count_test --progress=false

  # Run tailpipe query and verify the row count
  run tailpipe query "select count(*) as count from chaos_all_columns" --output csv
  echo $output

  assert_equal "$output" "count
5"

  # Clean up config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/chaos_all_columns.tpc
}

@test "verify partition with filter" {
  # Create a test partition configuration with filter
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/chaos_all_columns.tpc
partition "chaos_all_columns" "filter_test" {
  filter = "id % 2 = 0"
  source "chaos_all_columns" {
    row_count = 10
  }
}
EOF

  # Run tailpipe collect
  tailpipe collect chaos_all_columns.filter_test --progress=false

  # Run tailpipe query and verify the filtered data
  run tailpipe query "select count(*) as count from chaos_all_columns where id % 2 = 0" --output csv
  echo $output

  # Based on actual output - should be 5 rows (half of 10)
  assert_equal "$output" "count
5"

  # Clean up config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/chaos_all_columns.tpc
}

function teardown() {
  rm -rf $TAILPIPE_INSTALL_DIR/data
} 