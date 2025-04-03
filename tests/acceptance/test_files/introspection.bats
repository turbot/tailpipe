load "$LIB_BATS_ASSERT/load.bash"
load "$LIB_BATS_SUPPORT/load.bash"

@test "verify partition list shows created partitions" {
  # Create a test partition configuration using chaos tables
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/chaos_date_time.tpc
partition "chaos_date_time" "date_time_inc" {
  source "chaos_date_time" {
    row_count = 100
  }
}
EOF

  # Create another partition with different name
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/chaos_all_columns.tpc
partition "chaos_all_columns" "chaos_all_column_types" {
  source "chaos_all_columns" {
    row_count = 1
  }
}
EOF

  # Run partition list command
  run tailpipe partition list
  echo $output

  # Verify the output contains both partitions
  assert_output --partial "chaos_date_time.date_time_inc"
  assert_output --partial "chaos_all_columns.chaos_all_column_types"

  # Clean up config files
  rm -rf $TAILPIPE_INSTALL_DIR/config/chaos_date_time.tpc
  rm -rf $TAILPIPE_INSTALL_DIR/config/chaos_all_columns.tpc
}

@test "verify source list shows available sources" {

  # Run source list command
  run tailpipe source list
  echo $output

  # Verify the output contains the chaos source
  assert_output --partial "chaos_date_time"

  # Clean up config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/chaos_date_time.tpc
}

@test "verify partition list shows no partitions when none exist" {
  # Run partition list command with no partitions configured
  run tailpipe partition list --output json
  echo $output

  # Verify empty output is a valid JSON array
  assert_equal "$(echo "$output" | jq -r 'length')" "0"
}

@test "verify partition show displays correct JSON structure" {
  # Create a test partition configuration
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/chaos_all_columns.tpc
partition "chaos_all_columns" "all_column_types" {
  source "chaos_all_columns" {
    row_count = 1
  }
}
EOF

  # Run partition show command with JSON output
  run tailpipe partition show chaos_all_columns.all_column_types --output json
  echo $output

  # Verify the JSON structure using jq
  assert_equal "$(echo "$output" | jq -r '.[0].name')" "chaos_all_columns.all_column_types"
  assert_equal "$(echo "$output" | jq -r '.[0].plugin')" "hub.tailpipe.io/plugins/turbot/chaos@latest"
  assert_equal "$(echo "$output" | jq -r '.[0].local.file_count')" "0"
  assert_equal "$(echo "$output" | jq -r '.[0].local.file_size')" "0"

  # Clean up config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/chaos_all_columns.tpc
}

@test "verify source show displays correct JSON structure" {
  # Run source show command with JSON output
  run tailpipe source show chaos_all_columns --output json
  echo $output

  # Verify the JSON structure using jq
  assert_equal "$(echo "$output" | jq -r '.[0].name')" "chaos_all_columns"
}

@test "verify plugin show displays correct JSON structure" {
  # Run plugin show command with JSON output
  run tailpipe plugin show chaos --output json
  echo $output

  # Verify the JSON structure using jq
  assert_equal "$(echo "$output" | jq -r '.[0].name')" "hub.tailpipe.io/plugins/turbot/chaos@latest"
  # Update the expected values after actually adding a few format presets to the chaos plugin
  assert_equal "$(echo "$output" | jq -r '.[0].format_presets')" "null"
  # Update the expected values after actually adding a few format types to the chaos plugin
  assert_equal "$(echo "$output" | jq -r '.[0].format_types')" "null"
  
  # Verify tables array contains expected values
  # Update this when new tables are added to the chaos plugin
  assert_equal "$(echo "$output" | jq -r '.[0].tables | sort | join(",")')" "chaos_all_columns,chaos_date_time,chaos_struct_columns"
  
  # Verify sources array contains expected values
  # Update this when new sources are added to the chaos plugin
  assert_equal "$(echo "$output" | jq -r '.[0].sources | sort | join(",")')" "chaos_all_columns,chaos_date_time,chaos_struct_columns"
}
