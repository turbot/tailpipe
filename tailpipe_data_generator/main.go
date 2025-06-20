package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

const (
	baseDir            = "/Users/kai/tailpipe_data/dated"
	numAccounts        = 10
	numFilesPerAccount = 10
)

func main() {
	// Create the base directory
	err := os.MkdirAll(baseDir, 0755)
	if err != nil {
		fmt.Printf("Error creating base directory: %v\n", err)
		return
	}

	fmt.Println("Created base directory:", baseDir)

	// Create account directories and files
	for i := 1; i <= numAccounts; i++ {
		accountID := fmt.Sprintf("account%03d", i)
		accountDir := filepath.Join(baseDir, accountID)

		// Create account directory
		err := os.MkdirAll(accountDir, 0755)
		if err != nil {
			fmt.Printf("Error creating account directory %s: %v\n", accountID, err)
			continue
		}

		fmt.Println("Created account directory:", accountDir)

		// Create files in the account directory
		for j := 1; j <= numFilesPerAccount; j++ {
			// Get deterministic date from the last 10 days based on index
			// Using modulo to ensure we cycle through the days
			dayIndex := j % 10
			fileDate := getDateFromLast10Days(dayIndex)
			year := fileDate.Year()
			month := int(fileDate.Month())
			day := fileDate.Day()

			// Create filename in the format: account_id_year_month_day_idx.log
			filename := fmt.Sprintf("%s_%d_%02d_%02d_%02d.log", accountID, year, month, day, j)
			filePath := filepath.Join(accountDir, filename)

			// Create file with some random content
			content := generateRandomLogContent(accountID, fileDate, 10+rand.Intn(20))
			err := os.WriteFile(filePath, []byte(content), 0644)
			if err != nil {
				fmt.Printf("Error creating file %s: %v\n", filename, err)
				continue
			}

			fmt.Printf("Created file: %s\n", filePath)
		}
	}

}

// Get a specific date from the last 10 days based on index (0-9)
func getDateFromLast10Days(dayIndex int) time.Time {
	now := time.Now()
	// Get midnight today
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	// Subtract the specified number of days (0 = today, 9 = 9 days ago)
	return today.AddDate(0, 0, -dayIndex)
}

// Generate log content with chronologically ordered timestamps
func generateRandomLogContent(accountID string, fileDate time.Time, numLines int) string {
	logLevels := []string{"INFO", "DEBUG", "WARN", "ERROR"}
	operations := []string{"READ", "WRITE", "UPDATE", "DELETE", "LOGIN", "LOGOUT", "PROCESS"}

	var content string

	// Get just the date part (year, month, day) and start at 6:00 AM
	year, month, day := fileDate.Date()
	startTime := time.Date(year, month, day, 6, 0, 0, 0, fileDate.Location())

	// Calculate time interval between log entries to spread them throughout the day (until 9:00 PM)
	dayDuration := 15 * time.Hour // 6:00 AM to 9:00 PM
	interval := dayDuration / time.Duration(numLines)

	// Add a small random variation to each interval (±30 seconds)
	// to make logs look more natural while maintaining chronological order
	maxVariation := 30 * time.Second

	currentTime := startTime

	for i := 0; i < numLines; i++ {
		// Add a small random variation to the timestamp to make it look more natural
		// but still maintain chronological order
		variation := time.Duration(rand.Int63n(int64(maxVariation))) - (maxVariation / 2)
		timestamp := currentTime.Add(variation)

		logLevel := logLevels[rand.Intn(len(logLevels))]
		operation := operations[rand.Intn(len(operations))]
		status := rand.Intn(2) == 0 // Random boolean

		statusStr := "SUCCESS"
		if !status {
			statusStr = "FAILURE"
		}

		line := fmt.Sprintf("[%s] %s: Operation %s for account %s completed with %s [timestamp=%s]\n",
			timestamp.Format("2006-01-02 15:04:05"),
			logLevel,
			operation,
			accountID,
			statusStr,
			timestamp.Format("2006-01-02T15:04:05.000Z07:00"))

		content += line

		// Advance to the next timestamp
		currentTime = currentTime.Add(interval)
	}

	return content
}
