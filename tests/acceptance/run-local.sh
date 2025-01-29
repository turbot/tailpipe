#!/bin/bash

MY_PATH="`dirname \"$0\"`"              # relative
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"  # absolutized and normalized

# TODO PSKR review all exports and remove unused ones in tailpipe
export TAILPIPE_INSTALL_DIR=$(mktemp -d)
export TZ=UTC
export WD=$(mktemp -d)

trap "cd -;code=$?;rm -rf $TAILPIPE_INSTALL_DIR; exit $code" EXIT

cd "$WD"
echo "Working directory: $WD"
# setup a tailpipe installation
echo "Install directory: $TAILPIPE_INSTALL_DIR"

# Temporarily disable 'exit on error' since we want to run the collect command and not exit if it fails
set +e
tailpipe collect > /dev/null 2>&1
check_status=$?
set -e

echo "Installation complete at $TAILPIPE_INSTALL_DIR"

# install chaos plugin
tailpipe plugin install chaos
echo "Installed CHAOS plugin"

if [ $# -eq 0 ]; then
  # Run all test files
  "$MY_PATH/run.sh"
else
  "$MY_PATH/run.sh" "${1}"
fi

