#!/bin/bash

set -e

OS=$1
ARCH=$2

# Define the extension URL based on OS and ARCH
EXTENSION_URL="http://extensions.duckdb.org/v1.0.0/${OS}_${ARCH}/json.duckdb_extension.gz"
EXTENSION_DIR="./internal/extensions" # Target directory for placing the extension

# Download the extension with error checking
curl -L -A "Mozilla/5.0" -o json.duckdb_extension.gz "${EXTENSION_URL}"
if file json.duckdb_extension.gz | grep -q "gzip compressed data"; then
    echo "Valid gzip file downloaded"
else
    echo "Invalid gzip file format"
    cat json.duckdb_extension.gz # Optional: inspect the content
    exit 1
fi

# Ensure the target directory exists
mkdir -p "${EXTENSION_DIR}"

# Unzip the extension and move it to the target directory
gunzip -f json.duckdb_extension.gz
mv json.duckdb_extension "${EXTENSION_DIR}/"

echo "Downloaded and moved json.duckdb_extension to ${EXTENSION_DIR}/"