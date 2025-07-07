#!/bin/sh
# This is a script to install dependencies/packages, create user, and assign necessary permissions in the amazonlinux 2023 container.
# Used in release smoke tests. 

# update yum and install required packages
yum install -y shadow-utils tar gzip ca-certificates jq

# Extract the tailpipe binary
tar -xzf /artifacts/linux.tar.gz -C /usr/local/bin

# Create user, since tailpipe cannot be run as root
useradd -m tailpipe
          
# Ensure the binary is executable and owned by tailpipe and is executable
chown tailpipe:tailpipe /usr/local/bin/tailpipe
chmod +x /usr/local/bin/tailpipe

# Ensure the script is executable
chown tailpipe:tailpipe /scripts/smoke_test.sh
chmod +x /scripts/smoke_test.sh 