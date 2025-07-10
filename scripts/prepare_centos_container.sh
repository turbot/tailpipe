#!/bin/sh
# This is a script to install dependencies/packages, create user, and assign necessary permissions in the CentOS Stream 9 container.
# Used in release smoke tests.

set -e

# update yum and install required packages
yum install -y epel-release tar ca-certificates jq curl --allowerasing

# Extract the tailpipe binary
tar -xzf /artifacts/linux.tar.gz -C /usr/local/bin

# Make the binary executable
chmod +x /usr/local/bin/tailpipe

# Create user, since tailpipe cannot be run as root
useradd -m tailpipe

# Make the scripts executable
chown tailpipe:tailpipe /scripts/smoke_test.sh
chmod +x /scripts/smoke_test.sh 