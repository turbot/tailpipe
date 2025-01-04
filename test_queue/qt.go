package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/destel/rill"
)

// Mock process function
func processChunk(ctx context.Context, chunkID int) (string, error) {
	fmt.Printf("Processing chunk ID: %d\n", chunkID)
	time.Sleep(2 * time.Second) // Simulate processing
	if chunkID%10 == 0 {
		return "", errors.New("simulated error") // Simulate an error for testing
	}
	return fmt.Sprintf("Chunk %d processed successfully", chunkID), nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Simulate incoming chunks and wrap them in rill.Try[int]
	go func() {
		for i := 1; i <= 50; i++ {
			chunkIDs <- rill.Try[int]{Value: i}
			time.Sleep(200 * time.Millisecond) // Simulate incoming chunks
		}
		close(chunkIDs)
	}()

	// Wrap the channel using rill.FromChan
	stream := rill.Map(rill.FromChan(chunkIDs, nil), 5, func(chunkIDs []int) (string, error) {
		return processChunk(ctx, chunkID)
	})

	// Consume and handle results
	err := rill.ForEach(stream, 2, func(result string) error {
		fmt.Println(result)
		return nil
	})

	// Check for errors in the processing pipeline
	if err != nil {
		fmt.Printf("Pipeline error: %v\n", err)
	} else {
		fmt.Println("All chunks processed successfully!")
	}
}
