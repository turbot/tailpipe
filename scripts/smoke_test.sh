#!/bin/sh
# This is a script with set of commands to smoke test a tailpipe build.
# The plan is to gradually add more tests to this script.

set -e

# Ensure the PATH includes the directory where jq is installed
export PATH=$PATH:/usr/local/bin:/usr/bin:/bin

# Check jq is available
jq --version

/usr/local/bin/tailpipe --version # check version

# Test basic query functionality (should work without data)
/usr/local/bin/tailpipe query "SELECT 1 as smoke_test" # verify basic query works

# Test connect functionality
DB_FILE=$(/usr/local/bin/tailpipe connect --output json | jq -r '.database_filepath')

# Verify the database file exists
if [ -f "$DB_FILE" ]; then
    echo "Database file exists"
else
    echo "Database file not found: $DB_FILE"
    exit 1
fi

# Test plugin installation
/usr/local/bin/tailpipe plugin install chaos # install chaos plugin for testing
/usr/local/bin/tailpipe plugin list # verify plugin is installed

# Show available tables and sources after plugin installation
/usr/local/bin/tailpipe table list # should now show chaos tables
/usr/local/bin/tailpipe source list # should now show chaos sources

# Create configuration for testing
# the config path is different for darwin and linux
if [ "$(uname -s)" = "Darwin" ]; then
    CONFIG_DIR="/Users/runner/.tailpipe/config"
else
    CONFIG_DIR="/home/tailpipe/.tailpipe/config"
fi

mkdir -p "$CONFIG_DIR"

# Create chaos.tpc configuration file
cat > "$CONFIG_DIR/chaos.tpc" << 'EOF'
partition "chaos_date_time" "chaos_date_time_range" {
  source "chaos_date_time" {
    row_count = 100
  }
}
EOF

cat "$CONFIG_DIR/chaos.tpc"

# Test partition listing after adding configuration
/usr/local/bin/tailpipe partition list # should now show the chaos partition

# Show partition details
/usr/local/bin/tailpipe partition show chaos_date_time.chaos_date_time_range

# Test data collection - this is the main goal!
/usr/local/bin/tailpipe collect chaos_date_time.chaos_date_time_range

# Test querying collected data
# Query 1: Count total rows
/usr/local/bin/tailpipe query "SELECT COUNT(*) as total_rows FROM chaos_date_time" --output json

# Query 2: Show first 5 rows
/usr/local/bin/tailpipe query "SELECT * FROM chaos_date_time LIMIT 5" --output table

# Query 3: Basic aggregation
/usr/local/bin/tailpipe query "SELECT date_part('hour', datetime_col) as hour, COUNT(*) as count FROM chaos_date_time GROUP BY date_part('hour', datetime_col) ORDER BY hour LIMIT 5" --output json

# Test plugin show functionality
/usr/local/bin/tailpipe plugin show chaos
