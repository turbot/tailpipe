#!/bin/sh
# This is a script with set of commands to smoke test a tailpipe build.
# The plan is to gradually add more tests to this script.

set -e

# Ensure the PATH includes the directory where jq is installed
export PATH=$PATH:/usr/local/bin:/usr/bin:/bin

# Print PATH for debugging
echo "PATH is: $PATH"

# Check jq is available
jq --version

/usr/local/bin/tailpipe --version # check version

# Test basic plugin operations
/usr/local/bin/tailpipe plugin list # verify plugin list (should be empty initially)

# Test table and source listings (may be empty but should not error)
/usr/local/bin/tailpipe table list # verify table list
/usr/local/bin/tailpipe source list # verify source list
/usr/local/bin/tailpipe partition list # verify partition list
/usr/local/bin/tailpipe format list # verify format list
