package tailpipe

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/turbot/tailpipe-plugin-sdk/observer"
	"github.com/turbot/tailpipe-plugin-sdk/source"
)

func TestSourceInit(t *testing.T) {
	ctx := context.Background()
	s, err := NewSource(ctx, "mock", "test")
	require.NoError(t, err)
	assert.Equal(t, "test", s.Name)
}

func TestSourceCustomPluginDiscoverArtifactsEvents(t *testing.T) {
	ctx := context.Background()
	done := make(chan struct{})
	startCount := 0
	endCount := 0
	progressPoints := make([]int, 0)
	discoveredArtifacts := make(map[string]*source.ArtifactInfo)
	o := NewObserver(func(ei observer.Event) {
		switch e := ei.(type) {
		case *EventDiscoverArtifactsStart:
			startCount++
		case *EventDiscoverArtifactsProgress:
			progressPoints = append(progressPoints, e.Percentage())
		case *EventArtifactDiscovered:
			discoveredArtifacts[e.ArtifactInfo.Name] = e.ArtifactInfo
		case *EventDiscoverArtifactsEnd:
			endCount++
			close(done)
		}
	}).Start()
	//p := &MockSourcePlugin{}
	s, _ := NewSource(ctx, "mock", "test", WithSourceObservers(o))
	err := s.DiscoverArtifacts()
	assert.Nil(t, err)
	select {
	case <-done:
	case <-time.After(1 * time.Second):
	}
	assert.Equal(t, 1, startCount)
	assert.Equal(t, 1, endCount)
	assert.Equal(t, []int{34, 67, 100}, progressPoints)
	assert.Equal(t, mockCustomPluginArtifactInfos(), discoveredArtifacts)
}

func TestSourceCustomPluginDiscoverySafetyTimeout(t *testing.T) {
	ctx := context.Background()
	done := make(chan struct{})
	var discoveryErr error
	o := NewObserver(func(ei observer.Event) {
		switch e := ei.(type) {
		case *EventDiscoverArtifactsEnd:
			discoveryErr = e.Error
			close(done)
		}
	}).Start()
	//p := &MockSourcePlugin{}
	s, _ := NewSource(ctx, "mock", "test", WithSourceObservers(o))
	s.DiscoveryTimeout = 1
	require.NoError(t, s.DiscoverArtifacts())
	select {
	case <-done:
	case <-time.After(1 * time.Second):
	}
	assert.ErrorContains(t, discoveryErr, "discovery safety timeout exceeded")
}

func TestSourceDiscoveryErrorOnParallelRunTimeout(t *testing.T) {
	ctx := context.Background()
	//p := &MockSourcePlugin{}
	s, _ := NewSource(ctx, "mock", "test")
	// Set a ridiculously low timeout to force an error rather than
	// just waiting our turn.
	s.DiscoveryConcurrencyTimeout = 1
	require.NoError(t, s.DiscoverArtifacts())
	assert.ErrorContains(t, s.DiscoverArtifacts(), "context deadline exceeded")
}

func TestSourceDiscoveryRunsAreQueuedByDefault(t *testing.T) {
	ctx := context.Background()
	done := make(chan struct{})
	events := []observer.Event{}
	var wg sync.WaitGroup
	o := NewObserver(func(ei observer.Event) {
		switch e := ei.(type) {
		case *EventDiscoverArtifactsStart:
			events = append(events, e)
		case *EventDiscoverArtifactsEnd:
			events = append(events, e)
			wg.Done()
		}
	}).Start()
	//p := &MockSourcePlugin{}
	s, _ := NewSource(ctx, "mock", "test", WithSourceObservers(o))
	wg.Add(1)
	require.NoError(t, s.DiscoverArtifacts())
	wg.Add(1)
	require.NoError(t, s.DiscoverArtifacts())
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
	}
	assert.Len(t, events, 4)
	assert.IsType(t, &EventDiscoverArtifactsStart{}, events[0])
	assert.IsType(t, &EventDiscoverArtifactsEnd{}, events[1])
	assert.IsType(t, &EventDiscoverArtifactsStart{}, events[2])
	assert.IsType(t, &EventDiscoverArtifactsEnd{}, events[3])
}

func TestSourceDiscoveryRunsAreNotQueuedWhenConcurrencyIsAboveOne(t *testing.T) {
	ctx := context.Background()
	done := make(chan struct{})
	events := []observer.Event{}
	var wg sync.WaitGroup
	o := NewObserver(func(ei observer.Event) {
		switch e := ei.(type) {
		case *EventDiscoverArtifactsStart:
			events = append(events, e)
		case *EventDiscoverArtifactsEnd:
			events = append(events, e)
			wg.Done()
		}
	}).Start()
	//p := &MockSourcePlugin{}
	s, _ := NewSource(ctx, "mock", "test", WithSourceObservers(o), WithDiscoveryConcurrency(2))
	wg.Add(1)
	require.NoError(t, s.DiscoverArtifacts())
	wg.Add(1)
	require.NoError(t, s.DiscoverArtifacts())
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
	}
	assert.Len(t, events, 4)
	assert.IsType(t, &EventDiscoverArtifactsStart{}, events[0])
	assert.IsType(t, &EventDiscoverArtifactsStart{}, events[1])
	assert.IsType(t, &EventDiscoverArtifactsEnd{}, events[2])
	assert.IsType(t, &EventDiscoverArtifactsEnd{}, events[3])
}
