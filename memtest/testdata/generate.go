package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

// DynamicRow represents a row with dynamic fields, matching the SDK's structure
type DynamicRow struct {
	// The output columns, as a map of string to interface{}
	OutputColumns map[string]interface{}
}

// MarshalJSON overrides JSON serialization to include the dynamic columns
func (l *DynamicRow) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.OutputColumns)
}

type IndexDate struct {
	Index string
	Date  time.Time
}

func main() {
	// Parse command line arguments
	filename := flag.String("file", "test_bad_format.jsonl", "Output filename")
	rowCount := flag.Int("rows", 1000, "Number of rows to generate")
	colCount := flag.Int("cols", 100, "Number of data columns")
	indexCount := flag.Int("indexes", 5, "Number of distinct tp_index values")
	datesPerIndex := flag.Int("dates", 5, "Number of dates per index")
	flag.Parse()

	// Create the output file
	file, err := os.Create(*filename)
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	// Generate random seed
	rand.Seed(time.Now().UnixNano())

	// Generate column names (for dynamically added fields)
	// Based on what we observed in the bad.jsonl file, these would be fields like
	// savings_plan_total, savings_plan_used, etc.
	columnNames := []string{
		"account_id", "account_name", "billing_entity", "billing_period_start", "billing_period_end",
		"bill_type", "cost_category", "currency_code", "invoice_id", "legal_entity_name",
		"line_item_description", "line_item_normalized_usage_amount", "line_item_tax_type",
		"line_item_unblended_cost", "line_item_unblended_rate", "line_item_usage_account_id",
		"line_item_usage_amount", "line_item_usage_end_date", "line_item_usage_start_date",
		"line_item_usage_type", "pricing_term", "product_code", "product_family", "product_from_location",
		"product_location", "product_name", "product_to_location", "provider", "reservation_arn",
		"savings_plan_total_commitment_to_date", "savings_plan_used_commitment",
		"split_line_item_path", "split_line_item_split_percentange", "tags", "usage_type",
	}

	// Add numeric columns for the remaining count requested
	for i := len(columnNames); i < *colCount; i++ {
		columnNames = append(columnNames, fmt.Sprintf("column_%d", i))
	}

	// Generate index/date combinations
	combos := make([]IndexDate, 0)
	startDate := time.Now().AddDate(1, 0, 0) // Future date

	// For each index, generate multiple dates
	for i := 0; i < *indexCount; i++ {
		// Using a numeric string like in the bad file (e.g., 339713003993)
		index := fmt.Sprintf("%d", 339713000000+rand.Intn(10000))

		// Generate dates for this index
		for j := 0; j < *datesPerIndex; j++ {
			// Spread dates over a month
			date := startDate.AddDate(0, 0, j*(30/(*datesPerIndex)))
			combos = append(combos, IndexDate{
				Index: index,
				Date:  date,
			})
		}
	}

	// Create a slice to track which combinations have been used
	usedCombos := make([]bool, len(combos))
	combosUsed := 0

	// Generate and write rows
	for i := 0; i < *rowCount; i++ {
		// Create a row with OutputColumns to match SDK's DynamicRow
		row := &DynamicRow{
			OutputColumns: make(map[string]interface{}),
		}

		// Add the fixed fields
		row.OutputColumns["tp_id"] = fmt.Sprintf("d%drpup%dgmmlsnjf%c", rand.Intn(10), rand.Intn(10), 'a'+rand.Intn(26))
		row.OutputColumns["tp_source_type"] = "file"
		row.OutputColumns["tp_ingest_timestamp"] = time.Now().AddDate(1, 0, 0).Format(time.RFC3339)

		// Add dynamic fields
		for _, col := range columnNames {
			// Set different types of data based on column name pattern
			if strings.Contains(col, "cost") || strings.Contains(col, "amount") ||
				strings.Contains(col, "rate") || strings.Contains(col, "commitment") {
				// Numeric values as strings
				row.OutputColumns[col] = strconv.FormatFloat(rand.Float64()*100, 'f', 2, 64)
			} else if strings.Contains(col, "date") {
				// Date values
				date := time.Now().AddDate(0, 0, -rand.Intn(30))
				row.OutputColumns[col] = date.Format(time.RFC3339)
			} else if strings.Contains(col, "id") || strings.Contains(col, "arn") {
				// ID values
				row.OutputColumns[col] = fmt.Sprintf("id-%d-%d", rand.Intn(1000), rand.Intn(1000))
			} else {
				// Default string values
				row.OutputColumns[col] = fmt.Sprintf("value_%d_%d", i, rand.Intn(1000))
			}
		}

		// Select a combination ensuring we use all combinations
		var combo IndexDate
		if combosUsed < len(combos) {
			// Use each combination at least once
			for j := 0; j < len(combos); j++ {
				if !usedCombos[j] {
					combo = combos[j]
					usedCombos[j] = true
					combosUsed++
					break
				}
			}
		} else {
			// After using all combinations, randomly select from them
			combo = combos[rand.Intn(len(combos))]
		}

		// Set the partition fields
		row.OutputColumns["tp_index"] = combo.Index
		row.OutputColumns["tp_date"] = combo.Date.Format("2006-01-02 15:04:05") // Format as observed in bad file

		// Add empty object field to help DuckDB parse the structure
		row.OutputColumns["resource_tags"] = map[string]interface{}{}

		// Convert to JSON
		jsonData, err := json.Marshal(row)
		if err != nil {
			log.Fatalf("Failed to marshal JSON: %v", err)
		}

		// Write to file
		if _, err := file.Write(jsonData); err != nil {
			log.Fatalf("Failed to write to file: %v", err)
		}
		if _, err := file.WriteString("\n"); err != nil {
			log.Fatalf("Failed to write newline: %v", err)
		}
	}

	fmt.Printf("Generated %d rows with %d columns, %d indexes, and %d dates per index in flat format to %s\n",
		*rowCount, *colCount+5, *indexCount, *datesPerIndex, *filename) // +5 for the fixed fields
}
