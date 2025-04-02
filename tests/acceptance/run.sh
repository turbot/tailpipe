#!/bin/bash -e

MY_PATH="`dirname \"$0\"`"              # relative
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"  # absolutized and normalized

# trap "code=$?;rm -rf $MY_PATH/test_data/templates; exit $code" EXIT

# set this to the source file for development
export BATS_PATH=$MY_PATH/lib/bats/bin/bats
export LIB_BATS_ASSERT=$MY_PATH/lib/bats-assert
export LIB_BATS_SUPPORT=$MY_PATH/lib/bats-support
export TEST_DATA_DIR=$MY_PATH/test_data/templates

# Must have these commands for the test suite to run
declare -a required_commands=("sed" "tailpipe" $BATS_PATH "rm" "mv" "cp" "mkdir" "cd" "node" "npm" "npx" "head" "wc" "find" "basename" "dirname")

for required_command in "${required_commands[@]}"
do
  if [[ $(command -v $required_command | head -c1 | wc -c) -eq 0 ]]; then
    echo "$required_command is required for this test suite to run."
    exit -1
  fi
done

# create a copy of the test data templates
# mkdir $MY_PATH/test_data/templates
# export TEST_DATA_DIR=$MY_PATH/test_data/templates
# cp -R $MY_PATH/test_data/templates/* $TEST_DATA_DIR/

# cd $TEST_DATA_DIR

echo " ____  _             _   _               _____         _       "
echo "/ ___|| |_ __ _ _ __| |_(_)_ __   __ _  |_   _|__  ___| |_ ___ "
echo "\___ \| __/ _\` | '__| __| | '_ \ / _\` |   | |/ _ \/ __| __/ __|"
echo " ___) | || (_| | |  | |_| | | | | (_| |   | |  __/\__ \ |_\__ \\"
echo "|____/ \__\__,_|_|   \__|_|_| |_|\__, |   |_|\___||___/\__|___/"
echo "                                 |___/                         "

export PATH=$MY_PATH/lib/bats/bin:$PATH

if [[ ! ${TAILPIPE_INSTALL_DIR} ]];
then
  export TAILPIPE_INSTALL_DIR="$HOME/.tailpipe"
fi

batversion=$(bats --version)
echo $batversion
echo "Running with TAILPIPE_INSTALL_DIR set to: $TAILPIPE_INSTALL_DIR"
echo "Running with binary from: $(which tailpipe)"

if [ $# -eq 0 ]; then
  # Run all test files
  $BATS_PATH --tap $MY_PATH/test_files
else
  $BATS_PATH --tap $MY_PATH/test_files/${1}
fi
