load "$LIB_BATS_ASSERT/load.bash"
load "$LIB_BATS_SUPPORT/load.bash"

@test "verify file source logs count" {
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
200000'

  # remove the config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/cloudtrail_logs.tpc
}

@test "verify file source with multiple paths" {
  skip "TODO - This test is not working as expected. It needs to be fixed before it can be run."
  # Create a second directory with the same files for testing multiple paths  
  mkdir -p $SOURCE_FILES_DIR/aws_cloudtrail_flaws2/
  cp $SOURCE_FILES_DIR/aws_cloudtrail_flaws/* $SOURCE_FILES_DIR/aws_cloudtrail_flaws2/

  cat << EOF > $TAILPIPE_INSTALL_DIR/config/multi_path.tpc
partition "aws_cloudtrail_log" "fs" {
  source "file" {
    file_layout = ".json.gz"
    paths = ["$SOURCE_FILES_DIR/aws_cloudtrail_flaws/", "$SOURCE_FILES_DIR/aws_cloudtrail_flaws2/"]
  }
}
EOF

  cat $TAILPIPE_INSTALL_DIR/config/multi_path.tpc

  # tailpipe collect
  tailpipe collect aws_cloudtrail_log.fs --progress=false --from 2014-01-01

  # run tailpipe query and verify the row counts
  run tailpipe query "select count(*) as count from aws_cloudtrail_log;" --output csv
  echo $output

  # We expect double the records since we're reading from two identical directories
  assert_output --regexp 'count
400000'

  # remove the config file and test directory
  rm -rf $TAILPIPE_INSTALL_DIR/config/multi_path.tpc
  rm -rf $SOURCE_FILES_DIR/aws_cloudtrail_flaws2/
}

@test "verify file source with custom file layout" {
  skip "TODO - This test is not working as expected. It needs to be fixed before it can be run."
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/custom_layout.tpc
partition "aws_cloudtrail_log" "fs" {
  source "file" {
    paths = ["$SOURCE_FILES_DIR/aws_cloudtrail_flaws/"]
    file_layout = \`flaws_cloudtrail%{NUMBER:file_number}.json.gz\`
  }
}
EOF

  cat $TAILPIPE_INSTALL_DIR/config/custom_layout.tpc

  # tailpipe collect
  tailpipe collect aws_cloudtrail_log.fs --progress=false --from 2014-01-01

  # run tailpipe query and verify the row counts
  run tailpipe query "select count(*) as count from aws_cloudtrail_log;" --output csv
  echo $output

  # We expect the same number of records as the basic test
  assert_output --regexp 'count
200000'

  # remove the config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/custom_layout.tpc
}

@test "verify file source with custom patterns" {
  skip "TODO - This test is not working as expected. It needs to be fixed before it can be run."
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/custom_patterns.tpc
partition "aws_cloudtrail_log" "fs" {
  source "file" {
    paths = ["$SOURCE_FILES_DIR/aws_cloudtrail_flaws/"]
    file_layout = \`%{MY_PATTERN}.json.gz\`
    patterns = {
      "MY_PATTERN": \`flaws_cloudtrail%{NUMBER:file_number}\`
    }
  }
}
EOF

  cat $TAILPIPE_INSTALL_DIR/config/custom_patterns.tpc

  # tailpipe collect
  tailpipe collect aws_cloudtrail_log.fs --progress=false --from 2014-01-01

  # run tailpipe query and verify the row counts
  run tailpipe query "select count(*) as count from aws_cloudtrail_log;" --output csv
  echo $output

  # We expect the same number of records as the basic test
  assert_output --regexp 'count
200000'

  # remove the config file
  rm -rf $TAILPIPE_INSTALL_DIR/config/custom_patterns.tpc
}

function teardown() {
  rm -rf $TAILPIPE_INSTALL_DIR/data
} 