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
  run tailpipe query "select count(*) as count from chaos_all_columns" --output csv
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

@test "verify missing source block from partition" {
  skip "Re-enable after fixing the issue with missing source block"
  # Create a test partition configuration without a source block
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/missing_source_test.tpc
partition "chaos_all_columns" "missing_source_test" {
  # Intentionally missing source block
}
EOF

  # Run tailpipe collect and check for error message
  run tailpipe collect chaos_all_columns.missing_source_test --progress=false
  echo $output

  # Verify that the output contains the specific error message about missing source
  assert_output --partial "Partition chaos_all_columns.missing_source_test is missing required source block"
  assert_output --partial "A source block is required for every partition to specify the data source"

  # Clean up config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/missing_source_test.tpc
}

@test "verify partition with non-existent table name" {
  # Create a test partition configuration with a non-existent table name
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/invalid_table_test.tpc
partition "non_existent_table" "test_partition" {
  source "chaos_all_columns" {
    row_count = 10
  }
}
EOF

  # Run tailpipe collect and check for error message
  run tailpipe collect non_existent_table.test_partition --progress=false
  echo $output

  # Verify that the output contains the specific error message about invalid table
  assert_output --partial "error starting plugin non"
  assert_output --partial "no plugin installed matching 'non'"

  # Clean up config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/invalid_table_test.tpc
}

@test "verify partition with invalid table name format" {
  # Create a test partition configuration with an invalid table name format
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/invalid_format_test.tpc
partition "invalid.table.name" "test_partition" {
  source "chaos_all_columns" {
    row_count = 10
  }
}
EOF

  # Run tailpipe collect and check for error message
  run tailpipe collect invalid.table.name.test_partition --progress=false
  echo $output

  # Verify that the output contains the specific error message about invalid table name format
  assert_output --partial "Invalid name: A name must start with a letter or underscore and may contain only letters, digits, underscores, and dashes"

  # Clean up config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/invalid_format_test.tpc
}

@test "verify incompatible source type for table" {
  # Create a test partition configuration using an incompatible source type
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/incompatible_source_test.tpc
partition "chaos_date_time" "incompatible_source_test" {
  source "chaos_all_columns" {
    row_count = 10
  }
}
EOF

  # Run tailpipe collect and check for error message
  run tailpipe collect chaos_date_time.incompatible_source_test --progress=false
  echo $output

  # Verify that the output contains the specific error message about incompatible source
  assert_output --partial "source type chaos_all_columns not supported by table chaos_date_time"

  # Clean up config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/incompatible_source_test.tpc
}


function teardown() {
  rm -rf $TAILPIPE_INSTALL_DIR/data
} 