package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/destel/rill"
)

// Global job tracking variables (atomic)
var (
	maxIndex      int64                  = -1 // Tracks the latest job received
	nextIndex     int64                  = 0  // Tracks the next job to process
	processedJobs int64                  = 0  // Tracks the number of jobs processed
	mutex         sync.Mutex                  // Mutex to protect sync.Cond
	cond          = sync.NewCond(&mutex)      // Condition variable
	wg            sync.WaitGroup              // WaitGroup to track job completion
)

// Buffered channel for job indices
var jobQueue = make(chan int)

// Function to add a job (only updates maxIndex)
func receiveChunks() {
	atomic.AddInt64(&maxIndex, 1)
	fmt.Printf("Job received: %d\n", maxIndex)

	cond.L.Lock()
	cond.Broadcast()
	cond.L.Unlock()
}

// Enqueue jobs in order, running continuously
func scheduler(ctx context.Context) {
	// run goroutine which signals on a channel each time the cond is triggered
	jobChannel := make(chan struct{})
	go func() {
		for {
			cond.L.Lock()
			cond.Wait()
			cond.L.Unlock()

			select {
			case <-ctx.Done(): // Ensure clean exit
				fmt.Println("Stopping job enqueue signal listener.")
				return
			case jobChannel <- struct{}{}:
			}
		}
	}()

	// select
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Enqueue worker shutting down.")
			close(jobQueue) // Now it's safe to close the queue

			return
		case <-jobChannel:
			// if there are jobs to enqueue, do so
			currentNext := atomic.LoadInt64(&nextIndex)
			currentMax := atomic.LoadInt64(&maxIndex)

			for currentNext <= currentMax {
				wg.Add(1) // Track job being enqueued
				jobQueue <- int(currentNext)
				fmt.Printf("Job enqueued: %d\n", currentNext)
				atomic.AddInt64(&nextIndex, 1)
				// if there are jobs to enqueue, do so
				currentNext = atomic.LoadInt64(&nextIndex)
				currentMax = atomic.LoadInt64(&maxIndex)
			}
		}

	}
}

func main() {
	// Create a cancellable context for shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Start job enqueuing worker
	go scheduler(ctx)

	// Start the worker
	go workers(ctx)

	// Simulate incoming jobs
	for i := 0; i < 20; i++ {
		receiveChunks()

	}

	//go func() {
	//	time.Sleep(2 * time.Second)
	//	fmt.Println("Cancelling context.")
	//	cancel() // Cancel the context after 1 second
	//
	//}()
	// Wait for all jobs to be processed before exiting
	waitForCompletion(ctx, &wg)
	fmt.Printf("All jobs processed: cancelling")
	cancel()

	fmt.Println("Shutting down gracefully.")
}

func waitForCompletion(ctx context.Context, wg *sync.WaitGroup) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		fmt.Println("Context cancelled.")
	case <-done:
		fmt.Println("All jobs processed.")
	}
}

// Worker function that reads job indices from the channel
func workers(ctx context.Context) {
	ids := rill.FromChan(jobQueue, nil)

	_ = rill.ForEach(ids, 5, func(idx int) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		time.Sleep(5 * time.Second) // Simulate processing time
		fmt.Printf("Executed: UPDATE users SET last_active_at = NOW() WHERE id = %d\n", idx)
		atomic.AddInt64(&processedJobs, 1)
		wg.Done() // Mark job as processed
		return nil
	})
}
