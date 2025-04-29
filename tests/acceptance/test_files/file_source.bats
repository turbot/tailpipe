load "$LIB_BATS_ASSERT/load.bash"
load "$LIB_BATS_SUPPORT/load.bash"

@test "verify cloudtrail logs count" {
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/cloudtrail_logs.tpc
partition "aws_cloudtrail_log" "fs" {
  source "file" {
    file_layout = ".json.gz"
    paths = ["$SOURCE_FILES_DIR/aws_cloudtrail_flaws/"]
  }
}
EOF

  cat $TAILPIPE_INSTALL_DIR/config/cloudtrail_logs.tpc

  # tailpipe collect
  tailpipe collect aws_cloudtrail_log.fs --progress=false --from 2014-01-01

  # run tailpipe query and verify the row counts
  run tailpipe query "select count(*) as count from aws_cloudtrail_log;" --output csv
  echo $output

  # We expect at least some records from the two files
  assert_output --regexp 'count
"200,000"'

  # remove the config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/cloudtrail_logs.tpc
}

function teardown() {
  rm -rf $TAILPIPE_INSTALL_DIR/data
} 