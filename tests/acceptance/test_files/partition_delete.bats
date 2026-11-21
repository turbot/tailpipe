load "$LIB_BATS_ASSERT/load.bash"
load "$LIB_BATS_SUPPORT/load.bash"

@test "verify partition deletion" {
  # Create a test partition configuration
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/delete_test.tpc
partition "chaos_all_columns" "delete_test" {
  source "chaos_all_columns" {
    row_count = 20
  }
}
EOF

  # Run tailpipe collect to create the partition
  run tailpipe collect chaos_all_columns.delete_test --progress=false
  echo $output

  # Verify data has been collected by checking partition list
  run tailpipe partition list --output json
  echo $output
  
  # Use jq to check partition exists with correct row count
  local row_count=$(echo "$output" | jq '.[] | select(.name == "chaos_all_columns.delete_test") | .local.row_count')
  assert_equal "$row_count" "20"

  # Run partition delete
  run tailpipe partition delete chaos_all_columns.delete_test --force
  echo $output

  # Verify the partition was deleted by checking partition list
  run tailpipe partition list --output json
  echo $output
  
  # Use jq to verify partition has no data
  local file_count=$(echo "$output" | jq '.[] | select(.name == "chaos_all_columns.delete_test") | .local.file_count')
  local file_size=$(echo "$output" | jq '.[] | select(.name == "chaos_all_columns.delete_test") | .local.file_size')
  assert_equal "$file_count" "0"
  assert_equal "$file_size" "0"

  # Clean up config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/delete_test.tpc
}

@test "verify deletion of non-existent partition fails gracefully" {
  # Attempt to delete a partition that doesn't exist
  run tailpipe partition delete chaos_all_columns.non_existent_partition --force
  echo $output

  # Verify the command failed with appropriate error message
  assert_failure
  assert_output --partial "partition not found"
  
  # Verify the error message is user-friendly and clear
  assert_output --partial "chaos_all_columns.non_existent_partition"
}

@test "verify a partition removed from config can be listed, shown and deleted" {
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/removed_test.tpc
partition "chaos_all_columns" "removed_test" {
  source "chaos_all_columns" {
    row_count = 20
  }
}
EOF

  # collect the partition, then take it out of the config
  run tailpipe collect chaos_all_columns.removed_test --progress=false
  echo $output
  assert_success
  rm -rf $TAILPIPE_INSTALL_DIR/config/removed_test.tpc

  # partition list still shows it, marked as not configured, with its row count
  run tailpipe partition list --output json
  echo $output
  assert_success
  local row_count=$(echo "$output" | jq '.[] | select(.name == "chaos_all_columns.removed_test") | .local.row_count')
  local configured=$(echo "$output" | jq '.[] | select(.name == "chaos_all_columns.removed_test") | .configured')
  assert_equal "$row_count" "20"
  assert_equal "$configured" "false"

  # partition show finds it
  run tailpipe partition show chaos_all_columns.removed_test --output json
  echo $output
  assert_success
  assert_equal "$(echo "$output" | jq -r '.[0].configured')" "false"

  # plugin list counts it with its plugin
  run tailpipe plugin list --output json
  echo $output
  assert_success
  local plugin_partitions=$(echo "$output" | jq -r '.[] | select(.name | contains("chaos")) | .partitions | join(",")')
  assert_equal "$plugin_partitions" "chaos_all_columns.removed_test"

  # collecting it gives a clear error saying it is not configured
  run tailpipe collect chaos_all_columns.removed_test --from T-7d --progress=false
  echo $output
  assert_failure
  assert_output --partial "is not configured"

  # a delete of part of it is refused, as that needs the config
  run tailpipe partition delete chaos_all_columns.removed_test --from T-7d --force
  echo $output
  assert_failure
  assert_output --partial "is not configured"

  # the partition has a collection state file (the chaos source does not write one, so make one)
  local state_dir=$TAILPIPE_INSTALL_DIR/internal/collection/default
  local state_file=$state_dir/collection_state_chaos_all_columns_removed_test.json
  mkdir -p $state_dir
  echo '{}' > $state_file

  # a full delete removes its rows and its collection state
  run tailpipe partition delete chaos_all_columns.removed_test --force
  echo $output
  assert_success
  assert_output --partial "deleted 20 rows"
  assert [ ! -e $state_file ]

  # it no longer appears anywhere
  run tailpipe partition list --output json
  echo $output
  assert_success
  assert_equal "$(echo "$output" | jq '[.[]? | select(.name == "chaos_all_columns.removed_test")] | length')" "0"

  run tailpipe partition show chaos_all_columns.removed_test
  assert_failure

  run tailpipe plugin list --output json
  assert_success
  assert_equal "$(echo "$output" | jq -r '[.[] | select(.name | contains("chaos")) | .partitions[]?] | length')" "0"
}

function teardown() {
  rm -rf $TAILPIPE_INSTALL_DIR/data
} 