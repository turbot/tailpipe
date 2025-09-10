load "$LIB_BATS_ASSERT/load.bash"
load "$LIB_BATS_SUPPORT/load.bash"

## workspace tests

@test "generic config precedence test" {
  cp $SOURCE_FILES_DIR/config_tests/workspaces.tpc $TAILPIPE_INSTALL_DIR/config/workspaces.tpc
  # setup test folder and read the test-cases file
  cd $SOURCE_FILES_DIR/config_tests
  tests=$(cat workspace_tests.json)
  # echo $tests

  # to create the failure message
  err=""
  flag=0

  # fetch the keys(test names)
  test_keys=$(echo $tests | jq '. | keys[]')
  # echo $test_keys

  for i in $test_keys; do
    # each test case do the following
    unset TAILPIPE_INSTALL_DIR
    cwd=$(pwd)
    export TAILPIPE_CONFIG_DUMP=config_json

    # command accordingly
    cmd=$(echo $tests | jq -c ".[${i}]" | jq ".cmd")
    if [[ $cmd == '"query"' ]]; then
      tp_cmd='tailpipe query "select 1"'
    fi
    # echo $tp_cmd

    # key=$(echo $i)
    echo -e "\n"
    test_name=$(echo $tests | jq -c ".[${i}]" | jq ".test")
    echo ">>> TEST NAME: $test_name"

    # env variables needed for setup
    env=$(echo $tests | jq -c ".[${i}]" | jq ".setup.env")
    # echo $env

    # set env variables
    for e in $(echo "${env}" | jq -r '.[]'); do
      export $e
    done

    # args to run with tailpipe query command
    args=$(echo $tests | jq -c ".[${i}]" | jq ".setup.args")
    echo $args

    # construct the tailpipe command to be run with the args
    for arg in $(echo "${args}" | jq -r '.[]'); do
      tp_cmd="${tp_cmd} ${arg}"
    done
    echo "tailpipe command: $tp_cmd" # help debugging in case of failures

    # get the actual config by running the constructed tailpipe command
    run $tp_cmd
    echo "output from tailpipe command: $output" # help debugging in case of failures
    
    # The output contains log lines followed by a JSON object
    # Find the start of the JSON (line starting with '{') and extract from there to the end
    # Then use jq to parse and compact it
    json_start_line=$(echo "$output" | grep -n '^{' | tail -1 | cut -d: -f1)
    if [[ -n "$json_start_line" ]]; then
      config_json=$(echo "$output" | tail -n +$json_start_line)
    else
      # Fallback: try to find any JSON-like content
      config_json=$(echo "$output" | grep -A 1000 '{' | head -1000)
    fi
    
    # Parse with jq and handle errors gracefully
    actual_config=$(echo "$config_json" | jq -c '.' 2>/dev/null)
    if [[ $? -ne 0 ]] || [[ -z "$actual_config" ]]; then
      echo "Failed to parse JSON config, raw output:"
      echo "$config_json"
      actual_config="{}"
    fi
    echo "actual config: \n$actual_config" # help debugging in case of failures

    # get expected config from test case
    expected_config=$(echo $tests | jq -c ".[${i}]" | jq ".expected")
    # echo $expected_config

    # fetch only keys from expected config
    exp_keys=$(echo $expected_config | jq '. | keys[]' | jq -s 'flatten | @sh' | tr -d '\'\' | tr -d '"')

    for key in $exp_keys; do
      # get the expected and the actual value for the keys
      exp_val=$(echo $(echo $expected_config | jq --arg KEY $key '.[$KEY]' | tr -d '"'))
      act_val=$(echo $(echo $actual_config | jq --arg KEY $key '.[$KEY]' | tr -d '"'))

      # get the absolute paths for install-dir and mod-location
      if [[ $key == "install-dir" ]] || [[ $key == "mod-location" ]]; then
        exp_val="${cwd}/${exp_val}"
      fi
      echo "expected $key: $exp_val"
      echo "actual $key: $act_val"

      # check the values
      if [[ "$exp_val" != "$act_val" ]]; then
        flag=1
        err="FAILED: $test_name >> key: $key ; expected: $exp_val ; actual: $act_val \n${err}"
      fi
    done

    # check if all passed
    if [[ $flag -eq 0 ]]; then
      echo "PASSED ✅"
    else
      echo "FAILED ❌"
    fi
    # reset flag back to 0 for the next test case 
    flag=0
  done
  echo -e "\n"
  echo -e "$err"
  assert_equal "$err" ""
  rm -f err
}
