#!/bin/bash

# Create output CSV file with headers
timestamp=$(date '+%Y%m%d_%H%M%S')
output_file="memory_results_${timestamp}.csv"
echo "Creating CSV output file: $output_file"
echo "Filename,Rows,Cols,Partitions,MemoryMB" > "$output_file"

# Determine correct directory name
if [ -d "testsdata/generated" ]; then
    data_dir="testsdata/generated"
elif [ -d "testdata/generated" ]; then
    data_dir="testdata/generated"
else
    echo "Error: Neither testsdata/generated nor testdata/generated directory found"
    exit 1
fi

echo "Using data directory: $data_dir"
file_count=$(ls -1 $data_dir/*.jsonl 2>/dev/null | wc -l)
echo "Found $file_count files to process"

# Process each file in the directory
processed_count=0
success_count=0

for file in $data_dir/*.jsonl; do
    if [ -f "$file" ]; then
        filename=$(basename "$file")
        echo "Processing file $((processed_count+1))/$file_count: $filename"

        # Run the memtest app and capture its output
        output=$(./memtest "$file" 2>&1)
        exit_code=$?

        # Check if the command was successful
        if [ $exit_code -eq 0 ]; then
            # Try both formats of output (with or without spaces)
            csv_data=$(echo "$output" | grep -E '^[0-9]+,[ ]*[0-9]+,[ ]*[0-9]+,[ ]*[0-9]+$')
            if [ -z "$csv_data" ]; then
                csv_data=$(echo "$output" | grep -E '^[0-9]+, [0-9]+, [0-9]+, [0-9]+$')
            fi

            if [ -n "$csv_data" ]; then
                echo "$filename,$csv_data" >> "$output_file"
                success_count=$((success_count+1))
            else
                echo "Warning: Could not extract memory data from output"
            fi
        else
            echo "Error: memtest failed for $file"
        fi

        processed_count=$((processed_count+1))
    fi
done

echo "Processing complete. Results saved to $output_file"
echo "Processed $success_count/$file_count files successfully"