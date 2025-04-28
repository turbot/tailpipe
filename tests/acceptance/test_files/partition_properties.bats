load "$LIB_BATS_ASSERT/load.bash"
load "$LIB_BATS_SUPPORT/load.bash"

@test "verify partition with filter" {
  # Create a test partition configuration with filter
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/filter_test.tpc
partition "chaos_all_columns" "filter_test_1" {
  filter = "id % 2 = 0"
  source "chaos_all_columns" {
    row_count = 10
  }
}
EOF

  # Run tailpipe collect
  tailpipe collect chaos_all_columns.filter_test_1 --progress=false

  # Run tailpipe query and verify the filtered data
  run tailpipe query "select count(*) as count from chaos_all_columns where id % 2 = 0" --output csv
  echo $output

  # Based on actual output - should be 5 rows (half of 10)
  assert_equal "$output" "count
5"

  # Clean up config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/filter_test.tpc
}

@test "verify duplicate partition names" {
  # Create a test partition configuration with duplicate partition names
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/duplicate_test.tpc
partition "chaos_all_columns" "duplicate_test_1" {
  source "chaos_all_columns" {
    row_count = 5
  }
}

partition "chaos_all_columns" "duplicate_test_1" {
  source "chaos_all_columns" {
    row_count = 10
  }
}
EOF

  # Run tailpipe collect and check for error message
  run tailpipe collect chaos_all_columns.duplicate_test_1 --progress=false
  echo $output

  # Verify that the output contains the specific error message about duplicate partition
  assert_output --partial "partition duplicate_test_1 already exists for table chaos_all_columns"

  # Clean up config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/duplicate_test.tpc
}

@test "verify invalid filter syntax" {
  # Create a test partition configuration with invalid filter
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/invalid_filter_test.tpc
partition "chaos_all_columns" "invalid_filter_test_1" {
  filter = "invalid_column = 1"  # This column doesn't exist
  source "chaos_all_columns" {
    row_count = 5
  }
}
EOF

  # Run tailpipe collect and check for error message
  run tailpipe collect chaos_all_columns.invalid_filter_test_1 --progress=false
  echo $output

  # Verify that the output contains the specific error message about the invalid filter
  assert_output --partial "Binder Error: Referenced column \"invalid_column\" not found in FROM clause!"
  assert_output --partial "Errors:   5"  # Verify that there were errors in processing

  # Clean up config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/invalid_filter_test.tpc
}

@test "verify non-existent source reference" {
  # Create a test partition configuration with non-existent source
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/invalid_source_test.tpc
partition "chaos_all_columns" "invalid_source_test" {
  source "non_existent_source" {
    row_count = 5
  }
}
EOF

  # Run tailpipe collect and check for error message
  run tailpipe collect chaos_all_columns.invalid_source_test --progress=false
  echo $output

  # Verify that the output contains the specific error message about the non-existent source
  assert_output --partial "error starting plugin 'non' required for source 'non_existent_source'"
  assert_output --partial "no plugin installed matching 'non'"

  # Clean up config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/invalid_source_test.tpc
}

function teardown() {
  rm -rf $TAILPIPE_INSTALL_DIR/data
} 