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
  rm -f $TAILPIPE_INSTALL_DIR/config/cloudtrail_logs.tpc
}

@test "verify file source with multiple paths" {
  # Create a second directory with the same files for testing multiple paths  
  mkdir -p $SOURCE_FILES_DIR/aws_cloudtrail_flaws2/
  cp $SOURCE_FILES_DIR/aws_cloudtrail_flaws/* $SOURCE_FILES_DIR/aws_cloudtrail_flaws2/

  cat << EOF > $TAILPIPE_INSTALL_DIR/config/multi_path.tpc
partition "aws_cloudtrail_log" "fs2" {
  source "file" {
    file_layout = ".json.gz"
    paths = ["$SOURCE_FILES_DIR/aws_cloudtrail_flaws/", "$SOURCE_FILES_DIR/aws_cloudtrail_flaws2/"]
  }
}
EOF

  cat $TAILPIPE_INSTALL_DIR/config/multi_path.tpc
  ls -al $SOURCE_FILES_DIR/aws_cloudtrail_flaws2

  tailpipe plugin list

  # tailpipe collect
  tailpipe collect aws_cloudtrail_log.fs2 --progress=false --from 2014-01-01

  # run tailpipe query and verify the row counts
  run tailpipe query "select count(*) as count from aws_cloudtrail_log;" --output csv
  echo $output

  # We expect double the records since we're reading from two identical directories
  assert_output --regexp 'count
400000'

  # remove the config file and test directory
  rm -f $TAILPIPE_INSTALL_DIR/config/multi_path.tpc
  rm -rf $SOURCE_FILES_DIR/aws_cloudtrail_flaws2/
}

@test "verify file source with custom file layout" {
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/custom_layout.tpc
partition "aws_cloudtrail_log" "fs3" {
  source "file" {
    paths = ["$SOURCE_FILES_DIR/aws_cloudtrail_flaws/"]
    file_layout = \`flaws_cloudtrail%{NUMBER:file_number}.json.gz\`
  }
}
EOF

  cat $TAILPIPE_INSTALL_DIR/config/custom_layout.tpc

  # tailpipe collect
  tailpipe collect aws_cloudtrail_log.fs3 --progress=false --from 2014-01-01

  # run tailpipe query and verify the row counts
  run tailpipe query "select count(*) as count from aws_cloudtrail_log;" --output csv
  echo $output

  # We expect the same number of records as the basic test
  assert_output --regexp 'count
200000'

  # remove the config file
  rm -f $TAILPIPE_INSTALL_DIR/config/custom_layout.tpc
}

@test "verify file source with custom patterns" {
  cat << EOF > $TAILPIPE_INSTALL_DIR/config/custom_patterns.tpc
partition "aws_cloudtrail_log" "fs4" {
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
  tailpipe collect aws_cloudtrail_log.fs4 --progress=false --from 2014-01-01

  # run tailpipe query and verify the row counts
  run tailpipe query "select count(*) as count from aws_cloudtrail_log;" --output csv
  echo $output

  # We expect the same number of records as the basic test
  assert_output --regexp 'count
200000'

  # remove the config file
  rm -f $TAILPIPE_INSTALL_DIR/config/custom_patterns.tpc
}

function teardown() {
  rm -rf $TAILPIPE_INSTALL_DIR/data
} 