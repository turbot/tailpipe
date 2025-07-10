#!/bin/sh
# This is a script to install dependencies/packages, create user, and assign necessary permissions in the Amazon Linux 2023 container.
# Used in release smoke tests.

set -e  # Exit on any error

# update yum and install required packages
yum update -y
yum install -y tar ca-certificates jq

# Extract the tailpipe binary
tar -xzf /artifacts/linux.tar.gz -C /usr/local/bin

# Make the binary executable
chmod +x /usr/local/bin/tailpipe

# Create user, since tailpipe cannot be run as root
echo "Creating tailpipe user..."
useradd -m tailpipe

# Verify user was created
echo "Verifying user creation..."
id tailpipe
ls -la /home/tailpipe

# Make the scripts executable
chown tailpipe:tailpipe /scripts/smoke_test.sh
chmod +x /scripts/smoke_test.sh

echo "Amazon Linux container preparation completed successfully" 