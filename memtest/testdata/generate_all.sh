#!/bin/bash

# Create output directory
mkdir -p generated

# Array of row counts
ROWS=(1000 4000 8000)

# Array of column counts
# Array of column counts
COLUMNS=(10 25 100)

# Array of index counts
INDEXES=(1 25 100 400)

# Array of dates per index
DATES=(10)

# Generate all combinations
for rows in "${ROWS[@]}"; do
    for cols in "${COLUMNS[@]}"; do
        for indexes in "${INDEXES[@]}"; do
            for dates in "${DATES[@]}"; do
                filename="generated/test_${rows}rows_${cols}cols_${indexes}indexes_${dates}dates.jsonl"
                echo "Generating $filename..."
                go run generate.go -file="$filename" -rows=$rows -cols=$cols -indexes=$indexes -dates=$dates
            done
        done
    done
done

echo "All files generated in the 'generated' directory"