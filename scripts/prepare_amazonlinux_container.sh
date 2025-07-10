#!/bin/sh
# This is a script to install dependencies/packages, create user, and assign necessary permissions in the Amazon Linux 2023 container.
# Used in release smoke tests.

set -e  # Exit on any error

# update yum and install required packages
yum install -y shadow-utils tar gzip ca-certificates jq curl --allowerasing

# Extract the tailpipe binary
tar -xzf /artifacts/linux.tar.gz -C /usr/local/bin

# Make the binary executable
chmod +x /usr/local/bin/tailpipe

# Make the scripts executable
chmod +x /scripts/smoke_test.sh

echo "Amazon Linux container preparation completed successfully" 