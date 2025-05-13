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

function teardown() {
  rm -rf $TAILPIPE_INSTALL_DIR/data
} 