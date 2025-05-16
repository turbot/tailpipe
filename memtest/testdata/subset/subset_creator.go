package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func main() {
	// Define the source file
	sourceFile := "/Users/kai/Dev/github/turbot/tailpipe/memtest/testdata/bad.jsonl"

	// Define the line counts for the subsets
	lineCounts := []int{100, 500, 1000, 2500, 5000, 7500, 9000}

	// Read the source file
	file, err := os.Open(sourceFile)
	if err != nil {
		log.Fatalf("Failed to open source file: %v", err)
	}
	defer file.Close()

	// Read all lines from the source file
	scanner := bufio.NewScanner(file)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading source file: %v", err)
	}

	totalLines := len(lines)
	fmt.Printf("Source file contains %d lines\n", totalLines)

	// Create output directory
	outputDir := filepath.Join(filepath.Dir(sourceFile), "subsets")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Generate subset files
	for _, count := range lineCounts {
		if count > totalLines {
			fmt.Printf("Skipping %d lines (requested more than available)\n", count)
			continue
		}

		outputFile := filepath.Join(outputDir, fmt.Sprintf("bad_%d.jsonl", count))
		fmt.Printf("Creating subset with %d lines: %s\n", count, outputFile)

		out, err := os.Create(outputFile)
		if err != nil {
			log.Fatalf("Failed to create output file %s: %v", outputFile, err)
		}

		writer := bufio.NewWriter(out)
		for i := 0; i < count; i++ {
			if i < len(lines) {
				fmt.Fprintln(writer, lines[i])
			}
		}

		writer.Flush()
		out.Close()
	}

	fmt.Println("Subset files created successfully in the 'subsets' directory")
	fmt.Println("You can now use these files in your application")
}
