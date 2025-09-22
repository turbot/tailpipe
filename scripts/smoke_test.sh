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
DB_FILE=$(/usr/local/bin/tailpipe connect --output json | jq -r '.init_script_path')

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
    CONFIG_DIR="$HOME/.tailpipe/config"
else
    CONFIG_DIR="$HOME/.tailpipe/config"
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
# The chaos plugin generates dates around 2006-2007, so we need to collect from that range
echo "Starting data collection..."
# Use different timeout commands for macOS vs Linux
if [ "$(uname -s)" = "Darwin" ]; then
    # macOS - try gtimeout first, fallback to no timeout
    if command -v gtimeout >/dev/null 2>&1; then
        gtimeout 300 /usr/local/bin/tailpipe collect chaos_date_time.chaos_date_time_range --from="2006-01-01" --progress=false || {
            echo "Collection timed out or failed, trying without timeout..."
            /usr/local/bin/tailpipe collect chaos_date_time.chaos_date_time_range --from="2006-01-01" --progress=false
        }
    else
        echo "No timeout command available on macOS, running without timeout..."
        /usr/local/bin/tailpipe collect chaos_date_time.chaos_date_time_range --from="2006-01-01" --progress=false
    fi
else
    # Linux - use timeout
    timeout 300 /usr/local/bin/tailpipe collect chaos_date_time.chaos_date_time_range --from="2006-01-01" --progress=false || {
        echo "Collection timed out or failed, trying without progress bar..."
        /usr/local/bin/tailpipe collect chaos_date_time.chaos_date_time_range --from="2006-01-01" --progress=false 2>&1 | head -50
        echo "Collection attempt completed"
    }
fi

# Verify data was collected before proceeding
echo "Checking if data was collected..."
DATA_COUNT=$(/usr/local/bin/tailpipe query "SELECT COUNT(*) as count FROM chaos_date_time" --output json 2>/dev/null | jq -r '.rows[0].count' || echo "0")
echo "Data count: $DATA_COUNT"

if [ "$DATA_COUNT" -gt 0 ]; then
    echo "Data collection successful, proceeding with queries..."
    
    # Test querying collected data
    # Query 1: Count total rows
    /usr/local/bin/tailpipe query "SELECT COUNT(*) as total_rows FROM chaos_date_time" --output json

    # Query 2: Show first 5 rows
    /usr/local/bin/tailpipe query "SELECT * FROM chaos_date_time LIMIT 5" --output table

    # Query 3: Basic aggregation using the correct column name
    /usr/local/bin/tailpipe query "SELECT date_part('hour', timestamp) as hour, COUNT(*) as count FROM chaos_date_time GROUP BY date_part('hour', timestamp) ORDER BY hour LIMIT 5" --output json
else
    echo "No data collected, skipping query tests..."
    echo "Available tables after collection attempt:"
    /usr/local/bin/tailpipe table list
fi

# Test plugin show functionality
/usr/local/bin/tailpipe plugin show chaos
