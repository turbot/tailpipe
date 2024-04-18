package tailpipe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/observer"
	"github.com/turbot/tailpipe-plugin-sdk/source"

	"golang.org/x/sync/semaphore"
)

// Source respresents a source of artifacts. It works with external source
// plugins to support an extensible list of sources. Each Source instance
// represents a single source of artifacts (e.g. file system, S3 bucket, etc)
// and has a single plugin. The Source provides discovery and download
// operations, using primitives defined in the plugin.
type Source struct {

	// Name of the source.
	Name string `json:"name"`

	Plugin string `json:"plugin"`

	Config json.RawMessage `json:"config"`

	// discoveryConcurrency is the maximum number of concurrent discovery
	// operations. Default is 1, preventing multiple discoveries from running
	// at the same time but allowing them to be queued.
	// May only be set at source creation time as it is used in the semaphore.
	discoveryConcurrency int64

	// DiscoveryConcurrencyTimeout is the maximum time (in milliseconds) to wait
	// for a discovery operation to start while queued behind existing discovery
	// operations. Default is 30 seconds.
	DiscoveryConcurrencyTimeout int64

	// DiscoveryTimeout is the maximum time (in milliseconds) to wait for a
	// discovery operation to complete. Default is 1 hour.
	DiscoveryTimeout int64

	// downloadArtifactConcurrency is the maximum number of concurrent artifact
	// download operations. Default is 10, preventing resource exhaustion but
	// allowing a reasonable number of downloads to be queued.
	// May only be set at source creation time as it is used in the semaphore.
	downloadArtifactConcurrency int64

	// DownloadArtifactConcurrencyTimeout is the maximum time (in milliseconds)
	// to wait for a artifact download operation to start while queued behind
	// existing artifact download operations. Default is 24 hours allowing for
	// large queues.
	DownloadArtifactConcurrencyTimeout int64

	// DownloadArtifactTimeout is the maximum time (in milliseconds) to wait for
	// a artifact download operation to complete. Default is 1 hour.
	DownloadArtifactTimeout int64

	// downloadConcurrency is the maximum number of concurrent download
	// operations. Default is 10, preventing resource exhaustion but allowing a
	// reasonable number of downloads to be queued.
	// May only be set at source creation time as it is used in the semaphore.
	downloadConcurrency int64

	// DownloadConcurrencyTimeout is the maximum time (in milliseconds)
	// to wait for a download operation to start while queued behind
	// existing download operations. Default is 24 hours allowing for
	// large queues.
	DownloadConcurrencyTimeout int64

	// DownloadTimeout is the maximum time (in milliseconds) to wait for a
	// download operation to complete. Default is 1 hour.
	DownloadTimeout int64

	// SafetyTimeoutMultiple is the multiple of the timeout to use as a safety
	// timeout. This is used to ensure that the timeout is enforced even if the
	// plugin does not handle it correctly. So, if the discovery timeout is 10
	// seconds and the safty multiple is 1.1 then the safety timeout is 11 seconds
	// (giving the discovery a chance to timeout gracefully from the plugin
	// first). Default is 1.1.
	SafetyTimeoutMultiple float64

	// ctx is a base context used throughout all source operations. Required as
	// part of source creation.
	ctx context.Context

	// observers is a list of observers that will be notified of events.
	observers      []observer.ObserverInterface
	observersMutex sync.RWMutex

	// pluginRef is the pluginRef that will be used to perform source operations.
	pluginRef source.Plugin

	// discoveryConcurrencySemaphore is a semaphore used to limit the number of
	// concurrent discovery operations.
	discoveryConcurrencySemaphore *semaphore.Weighted

	// downloadArtifactConcurrencySempahore is a semaphore used to limit the
	// number of concurrent artifact download operations.
	downloadArtifactConcurrencySempahore *semaphore.Weighted

	// downloadConcurrencySempahore is a semaphore used to limit the number of
	// concurrent download operations.
	downloadConcurrencySempahore *semaphore.Weighted
}

type SourceOption func(*Source) error

func NewSource(ctx context.Context, plugin string, name string, opts ...SourceOption) (*Source, error) {
	s := &Source{
		Plugin: plugin,
		Name:   name,
	}
	if err := s.Init(ctx, opts...); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Source) Init(ctx context.Context, opts ...SourceOption) error {

	s.ctx = ctx

	for _, opt := range opts {
		if err := opt(s); err != nil {
			return err
		}
	}

	if s.pluginRef == nil && s.Plugin != "" {
		sp := GetConfig().SourcePlugin(s.Plugin)
		if sp != nil {
			clonedSp, err := CloneSourcePlugin(sp)
			if err != nil {
				return err
			}
			s.pluginRef = clonedSp
			s.pluginRef.AddObserver(s)

			// Load the plugin-specific config
			if err := s.pluginRef.LoadConfig(s.Config); err != nil {
				return err
			}

			if err := s.pluginRef.ValidateConfig(); err != nil {
				return err
			}

		}
	}

	// Default values
	if s.discoveryConcurrency == 0 {
		s.discoveryConcurrency = 1
	}
	if s.DiscoveryConcurrencyTimeout == 0 {
		s.DiscoveryConcurrencyTimeout = 30 * 1000
	}
	if s.DiscoveryTimeout == 0 {
		s.DiscoveryTimeout = 60 * 60 * 1000
	}
	if s.downloadArtifactConcurrency == 0 {
		s.downloadArtifactConcurrency = 10
	}
	if s.DownloadArtifactConcurrencyTimeout == 0 {
		s.DownloadArtifactConcurrencyTimeout = 24 * 60 * 60 * 1000
	}
	if s.DownloadArtifactTimeout == 0 {
		s.DownloadArtifactTimeout = 60 * 60 * 1000
	}
	if s.downloadConcurrency == 0 {
		s.downloadConcurrency = 1
	}
	if s.DownloadConcurrencyTimeout == 0 {
		s.DownloadConcurrencyTimeout = 30 * 1000
	}
	if s.DownloadTimeout == 0 {
		s.DownloadTimeout = 4 * 60 * 60 * 1000
	}
	if s.SafetyTimeoutMultiple == 0 {
		s.SafetyTimeoutMultiple = 1.1
	}

	// Initialize concurrency controls
	if s.discoveryConcurrencySemaphore == nil {
		s.discoveryConcurrencySemaphore = semaphore.NewWeighted(s.discoveryConcurrency)
	}
	if s.downloadArtifactConcurrencySempahore == nil {
		s.downloadArtifactConcurrencySempahore = semaphore.NewWeighted(s.downloadArtifactConcurrency)
	}
	if s.downloadConcurrencySempahore == nil {
		s.downloadConcurrencySempahore = semaphore.NewWeighted(s.downloadConcurrency)
	}

	return s.Validate()
}

func (s *Source) Validate() error {
	if s.Name == "" {
		return errors.New("source name is required")
	}
	return nil
}

/*
func WithSourcePlugin(p source.Plugin) SourceOption {
	return func(c *Source) error {
		c.InitPlugin(p)
		return nil
	}
}
*/

func WithDiscoveryConcurrency(concurrency int64) SourceOption {
	return func(c *Source) error {
		c.discoveryConcurrency = concurrency
		return nil
	}
}

func WithDownloadConcurrency(concurrency int64) SourceOption {
	return func(c *Source) error {
		c.downloadArtifactConcurrency = concurrency
		return nil
	}
}

func WithSourceObservers(observers ...observer.ObserverInterface) SourceOption {
	return func(s *Source) error {
		s.observersMutex.Lock()
		defer s.observersMutex.Unlock()
		s.observers = append(s.observers, observers...)
		return nil
	}
}

func (s *Source) AddObserver(observer observer.ObserverInterface) *Source {
	s.observersMutex.Lock()
	defer s.observersMutex.Unlock()
	s.observers = append(s.observers, observer)
	return s
}

func (s *Source) RemoveObserver(observer observer.ObserverInterface) *Source {
	s.observersMutex.Lock()
	defer s.observersMutex.Unlock()
	for i, o := range s.observers {
		if o == observer {
			s.observers = append(s.observers[:i], s.observers[i+1:]...)
			break
		}
	}
	return s
}

func (s *Source) NotifyObservers(e observer.Event) {
	s.observersMutex.RLock()
	defer s.observersMutex.RUnlock()
	for _, o := range s.observers {
		o.Notify(e)
	}
}

func (s *Source) DiscoveryConcurrency() int64 {
	return s.discoveryConcurrency
}

func (s *Source) DownloadConcurrency() int64 {
	return s.downloadArtifactConcurrency
}

func (s *Source) GetPlugin() source.Plugin {
	return s.pluginRef
}

/*
func (s *Source) InitPlugin(p source.Plugin) *Source {
	s.pluginRef = p
	p.Init(s.ctx)
	p.AddObserver(s)
	return s
}
*/

func (s *Source) EnsurePlugin() (source.Plugin, error) {
	if s.Plugin == "" {
		return nil, errors.New("source plugin not defined")
	}
	p := s.GetPlugin()
	if p == nil {
		return nil, fmt.Errorf("source plugin not found: %s", s.Plugin)
	}
	return p, nil
}

func (s *Source) NotifyDiscoverArtifactsStart() {
	s.NotifyObservers(&EventDiscoverArtifactsStart{})
}

func (s *Source) NotifyDiscoverArtifactsProgress(current int64, total int64) {
	s.NotifyObservers(&EventDiscoverArtifactsProgress{current, total})
}

func (s *Source) NotifyDiscoverArtifactsEnd(err error) {
	s.NotifyObservers(&EventDiscoverArtifactsEnd{err})
}

func (s *Source) NotifyArtifactDiscovered(ai *source.ArtifactInfo) {
	s.NotifyObservers(&EventArtifactDiscovered{ai})
}

func (s *Source) NotifyDownloadArtifactStart(ai *source.ArtifactInfo) {
	s.NotifyObservers(&EventDownloadArtifactStart{ai})
}

func (s *Source) NotifyDownloadArtifactProgress(ai *source.ArtifactInfo, current int64, total int64) {
	s.NotifyObservers(&EventDownloadArtifactProgress{ai, current, total})
}

func (s *Source) NotifyDownloadArtifactEnd(ai *source.ArtifactInfo, err error) {
	s.NotifyObservers(&EventDownloadArtifactEnd{ai, err})
}

func (s *Source) NotifyArtifactDownloaded(a *source.Artifact) {
	s.NotifyObservers(&EventArtifactDownloaded{a})
}

func (s *Source) NotifyDownloadStart() {
	s.NotifyObservers(&EventDownloadStart{})
}

func (s *Source) NotifyDownloadProgress(current int64, total int64) {
	s.NotifyObservers(&EventDownloadProgress{current, total})
}

func (s *Source) NotifyDownloadEnd(err error) {
	s.NotifyObservers(&EventDownloadEnd{err})
}

func (s *Source) DiscoverArtifacts() error {
	if _, err := s.EnsurePlugin(); err != nil {
		return err
	}

	// Limit the number of concurrent discovery operations. The default is
	// 1 to prevent multiple discoveries in parallel - I don't see any reason
	// why that should ever be increased.
	// The timeout handles a second discovery being queued while the first
	// is still running, returning an error if it's not available fast enough.
	ctx, cancelSemaphoreTimeout := context.WithTimeout(s.ctx, time.Duration(s.DiscoveryConcurrencyTimeout)*time.Millisecond)
	if err := s.discoveryConcurrencySemaphore.Acquire(ctx, 1); err != nil {
		cancelSemaphoreTimeout()
		return err
	}

	go func() {
		defer cancelSemaphoreTimeout()
		defer s.discoveryConcurrencySemaphore.Release(1)

		// Set a timeout for the discovery operation. This should be handled
		// gracefully by the plugin.
		discoveryCtx, cancelTimeout := context.WithTimeout(s.ctx, time.Duration(s.DiscoveryTimeout)*time.Millisecond)
		defer cancelTimeout()

		// Although we want the plugin to handle timeout themselves, we also
		// need to ensure the timeout is enforced so we use a safety timeout
		// that is 10% longer than the download timeout.
		safetyTimeout := time.Duration(float64(s.DiscoveryTimeout) * s.SafetyTimeoutMultiple)
		safetyCtx, cancelSafetyTimeout := context.WithTimeout(ctx, safetyTimeout*time.Millisecond)
		defer cancelSafetyTimeout()

		s.NotifyDiscoverArtifactsStart()

		done := make(chan error, 1)
		go func() {
			p := s.GetPlugin()
			fmt.Println(p)
			err := p.DiscoverArtifacts(discoveryCtx)
			done <- err
		}()

		select {
		case err := <-done:
			s.NotifyDiscoverArtifactsEnd(err)
		case <-safetyCtx.Done():
			s.NotifyDiscoverArtifactsEnd(fmt.Errorf("discovery safety timeout exceeded"))
		}
	}()
	return nil
}

func (s *Source) DownloadArtifact(ai *source.ArtifactInfo) error {
	if _, err := s.EnsurePlugin(); err != nil {
		return err
	}

	// Limit the number of concurrent download operations. This is configurable
	// and used to prevent resource exhaustion.
	// If many downloads are queued the timeout will eventually clean them up.
	// This is set very high by default to allow large queued operations.
	ctx, cancelSemaphoreTimeout := context.WithTimeout(s.ctx, time.Duration(s.DownloadArtifactConcurrencyTimeout)*time.Millisecond)
	if err := s.downloadArtifactConcurrencySempahore.Acquire(ctx, 1); err != nil {
		cancelSemaphoreTimeout()
		return err
	}

	go func() {
		defer cancelSemaphoreTimeout()
		defer s.downloadArtifactConcurrencySempahore.Release(1)

		// Set a timeout for the download operation. This should be handled
		// gracefully by the plugin.
		downloadCtx, cancelTimeout := context.WithTimeout(s.ctx, time.Duration(s.DownloadArtifactTimeout)*time.Millisecond)
		defer cancelTimeout()

		// Although we want the plugin to handle timeout themselves, we also
		// need to ensure the timeout is enforced so we use a safety timeout
		// that is 10% longer than the download timeout.
		safetyTimeout := time.Duration(float64(s.DownloadArtifactTimeout) * s.SafetyTimeoutMultiple)
		safetyCtx, cancelSafetyTimeout := context.WithTimeout(downloadCtx, safetyTimeout*time.Millisecond)
		defer cancelSafetyTimeout()

		s.NotifyDownloadArtifactStart(ai)

		done := make(chan error, 1)
		go func() {
			done <- s.GetPlugin().DownloadArtifact(downloadCtx, ai)
		}()

		select {
		case err := <-done:
			s.NotifyDownloadArtifactEnd(ai, err)
		case <-safetyCtx.Done():
			s.NotifyDownloadArtifactEnd(ai, fmt.Errorf("download artifact safety timeout exceeded: %s", ai.Name))
		}
	}()

	return nil
}

func (s *Source) Download() error {
	if _, err := s.EnsurePlugin(); err != nil {
		return err
	}

	// Limit the number of concurrent download operations. This is configurable
	// and used to prevent resource exhaustion.
	// If many downloads are queued the timeout will eventually clean them up.
	// This is set very high by default to allow large queued operations.
	ctx, cancelSemaphoreTimeout := context.WithTimeout(s.ctx, time.Duration(s.DownloadConcurrencyTimeout)*time.Millisecond)
	if err := s.downloadConcurrencySempahore.Acquire(ctx, 1); err != nil {
		cancelSemaphoreTimeout()
		return err
	}

	go func() {
		defer cancelSemaphoreTimeout()
		defer s.downloadConcurrencySempahore.Release(1)

		// Set a timeout for the download operation. This should be handled
		// gracefully by the plugin.
		downloadCtx, cancelDownloadTimeout := context.WithTimeout(s.ctx, time.Duration(s.DownloadTimeout)*time.Millisecond)
		defer cancelDownloadTimeout()

		// Although we want the plugin to handle timeout themselves, we also
		// need to ensure the timeout is enforced so we use a safety timeout
		// that is 10% longer than the download timeout.
		safetyTimeout := time.Duration(float64(s.DownloadTimeout) * s.SafetyTimeoutMultiple)
		safetyCtx, cancelSafetyTimeout := context.WithTimeout(downloadCtx, safetyTimeout*time.Millisecond)
		defer cancelSafetyTimeout()

		var wg sync.WaitGroup

		downloadObserver := NewObserver(func(ei observer.Event) {
			// switch based on the struct of the event
			switch e := ei.(type) {
			case *EventArtifactDiscovered:
				wg.Add(1)
				go func() {
					if err := s.DownloadArtifact(e.ArtifactInfo); err != nil {
						// If we failed to start the download then we should
						// clear tracking for this download.
						// The actual error will be reported up to observers,
						// so we don't need to do anything here.
						wg.Done()
					}
				}()
			case *EventArtifactDownloaded:
				wg.Done()
			case *EventDiscoverArtifactsEnd:
				// Once discovery is complete, we can clear the wait for that.
				wg.Done()
			}
		}).Start()

		s.AddObserver(downloadObserver)
		defer s.RemoveObserver(downloadObserver)

		s.NotifyDownloadStart()

		// Open a wait on the discovery phase
		done := make(chan error, 1)
		wg.Add(1)
		if err := s.DiscoverArtifacts(); err != nil {
			// We didn't even start discovery, so clear the wait immediately.
			wg.Done()
			done <- err
		}

		go func() {
			wg.Wait()
			done <- nil
		}()

		select {
		case err := <-done:
			s.NotifyDownloadEnd(err)
		case <-safetyCtx.Done():
			s.NotifyDownloadEnd(fmt.Errorf("download safety timeout exceeded"))
		}
	}()

	return nil

}
