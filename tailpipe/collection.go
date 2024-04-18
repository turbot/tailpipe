package tailpipe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/collection"
	"github.com/turbot/tailpipe-plugin-sdk/observer"
	"github.com/turbot/tailpipe-plugin-sdk/source"
	"golang.org/x/sync/semaphore"
)

type Collection struct {
	Name        string          `json:"name"`
	Plugin      string          `json:"plugin"`
	Source      string          `json:"source"`
	Destination string          `json:"destination"`
	Config      json.RawMessage `json:"config"`

	// extractArtifactRowsConcurrency is the maximum number of concurrent extract rows
	// operations. Default is 10, preventing resource exhaustion but allowing a
	// reasonable number of extractions to be queued.
	// May only be set at source creation time as it is used in the semaphore.
	extractArtifactRowsConcurrency int

	// ExtractArtifactRowsConcurrencyTimeout is the maximum time (in milliseconds)
	// to wait for row extraction from an artifact to start while queued behind
	// existing artifact extract operations. Default is 24 hours allowing for
	// large queues.
	ExtractArtifactRowsConcurrencyTimeout int

	// ExtractArtifactRowsTimeout is the maximum time (in milliseconds) to wait for
	// row extraction from an artifact to complete. Default is 10 mins.
	ExtractArtifactRowsTimeout int

	syncArtifactConcurrency int

	SyncArtifactConcurrencyTimeout int

	SyncArtifactTimeout int

	// SafetyTimeoutMultiple is the multiple of the timeout to use as a safety
	// timeout. This is used to ensure that the timeout is enforced even if the
	// plugin does not handle it correctly. So, if the discovery timeout is 10
	// seconds and the safty multiple is 1.1 then the safety timeout is 11 seconds
	// (giving the discovery a chance to timeout gracefully from the plugin
	// first). Default is 1.1.
	SafetyTimeoutMultiple float64

	// ctx is a base context used throughout all collection operations. Required as
	// part of collection creation.
	ctx context.Context

	// observers is a list of observers that will be notified of events.
	observers []observer.ObserverInterface

	// Protect the observers list from being changed while it's being iterated over
	// to send events to observers. Without this protection it's possible for the
	// same event to be sent twice to the same observer.
	observersMutex sync.RWMutex

	//writers map[string]*writer.ParquetWriter

	writerManager *WriterManager
	//writerManager *DuckDBWriter

	pluginRef collection.Plugin

	sourceRef *Source

	destinationRef *Destination

	// extractArtifactRowsConcurrencySemaphore is a semaphore used to limit the
	// number of concurrent artifact extract operations.
	extractArtifactRowsConcurrencySempahore *semaphore.Weighted
	syncConcurrencySempahore                *semaphore.Weighted
}

type CollectionOption func(*Collection) error

func NewCollection(ctx context.Context, plugin string, name string, src string, opts ...CollectionOption) (*Collection, error) {
	c := &Collection{
		Plugin: plugin,
		Name:   name,
		Source: src,
	}
	if err := c.Init(ctx, opts...); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Collection) Init(ctx context.Context, opts ...CollectionOption) error {

	c.ctx = ctx

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return err
		}
	}

	if c.pluginRef == nil && c.Plugin != "" {
		cp := GetConfig().CollectionPlugin(c.Plugin)
		if cp != nil {
			clonedCp, err := CloneCollectionPlugin(cp)
			if err != nil {
				return err
			}
			c.pluginRef = clonedCp
			c.pluginRef.AddObserver(c)

			// Load the plugin-specific config
			if err := c.pluginRef.LoadConfig(c.Config); err != nil {
				return err
			}

			if err := c.pluginRef.ValidateConfig(); err != nil {
				return err
			}

		}
	}

	// Default values
	if c.Destination == "" {
		c.Destination = "default"
	}
	if c.extractArtifactRowsConcurrency == 0 {
		c.extractArtifactRowsConcurrency = 10
	}
	if c.ExtractArtifactRowsConcurrencyTimeout == 0 {
		c.ExtractArtifactRowsConcurrencyTimeout = 24 * 60 * 60 * 1000
	}
	if c.ExtractArtifactRowsTimeout == 0 {
		c.ExtractArtifactRowsTimeout = 10 * 60 * 1000
	}
	if c.syncArtifactConcurrency == 0 {
		c.syncArtifactConcurrency = 10
	}
	if c.SyncArtifactConcurrencyTimeout == 0 {
		c.SyncArtifactConcurrencyTimeout = 24 * 60 * 60 * 1000
	}
	if c.SyncArtifactTimeout == 0 {
		c.SyncArtifactTimeout = 10 * 60 * 1000
	}
	if c.SafetyTimeoutMultiple == 0 {
		c.SafetyTimeoutMultiple = 1.1
	}

	if c.writerManager == nil {
		c.writerManager = NewWriterManager(c)
	}

	// Initialize concurrency controls
	if c.extractArtifactRowsConcurrencySempahore == nil {
		c.extractArtifactRowsConcurrencySempahore = semaphore.NewWeighted(int64(c.extractArtifactRowsConcurrency))
	}
	if c.syncConcurrencySempahore == nil {
		c.syncConcurrencySempahore = semaphore.NewWeighted(int64(c.syncArtifactConcurrency))
	}

	return c.Validate()
}

func (c *Collection) Validate() error {
	if c.Name == "" {
		return errors.New("collection name is required")
	}
	if _, err := c.EnsurePlugin(); err != nil {
		return err
	}
	if _, err := c.EnsureDestination(); err != nil {
		return err
	}
	if _, err := c.EnsureSource(); err != nil {
		return err
	}
	return nil
}

func WithSource(s *Source) CollectionOption {
	return func(c *Collection) error {
		c.sourceRef = s
		return nil
	}
}

func WithDestination(d *Destination) CollectionOption {
	return func(c *Collection) error {
		c.destinationRef = d
		return nil
	}
}

/*
func WithCollectionPlugin(p collection.Plugin) CollectionOption {
	return func(c *Collection) error {
		c.Init(c.ctx)
		p.AddObserver(c)
		return nil
	}
}
*/

func WithCollectionObservers(observers ...observer.ObserverInterface) CollectionOption {
	return func(c *Collection) error {
		c.observersMutex.Lock()
		defer c.observersMutex.Unlock()
		c.observers = append(c.observers, observers...)
		return nil
	}
}

func (c *Collection) AddObserver(observer observer.ObserverInterface) *Collection {
	c.observersMutex.Lock()
	defer c.observersMutex.Unlock()
	c.observers = append(c.observers, observer)
	return c
}

func (c *Collection) RemoveObserver(observer observer.ObserverInterface) *Collection {
	c.observersMutex.Lock()
	defer c.observersMutex.Unlock()
	for i, o := range c.observers {
		if o == observer {
			c.observers = append(c.observers[:i], c.observers[i+1:]...)
			break
		}
	}
	return c
}

func (c *Collection) NotifyObservers(e observer.Event) {
	c.observersMutex.RLock()
	defer c.observersMutex.RUnlock()
	for _, o := range c.observers {
		o.Notify(e)
	}
}

func (c *Collection) Identifier() string {
	return c.Name
}

func (c *Collection) GetSource() *Source {
	return GetConfig().Source(c.Source)
}

func (c *Collection) EnsureSource() (*Source, error) {
	if c.Source == "" {
		return nil, errors.New("collection source not defined")
	}
	s := c.GetSource()
	if s == nil {
		return nil, errors.New("collection source not found")
	}
	return s, nil
}

func (c *Collection) GetDestination() *Destination {
	return GetConfig().Destination(c.Destination)
}

func (c *Collection) EnsureDestination() (*Destination, error) {
	if c.Destination == "" {
		return nil, errors.New("collection destination not defined")
	}
	d := c.GetDestination()
	if d == nil {
		return nil, errors.New("collection destination not found")
	}
	return d, nil
}

func (c *Collection) GetPlugin() collection.Plugin {
	return c.pluginRef
}

func (c *Collection) EnsurePlugin() (collection.Plugin, error) {
	if c.Plugin == "" {
		return nil, errors.New("collection plugin not defined")
	}
	p := c.GetPlugin()
	if p == nil {
		return nil, errors.New("collection plugin not found")
	}
	return p, nil
}

func (c *Collection) NotifyExtractArtifactRowsStart(a *source.Artifact) {
	c.NotifyObservers(&EventExtractRowsStart{a})
}

func (c *Collection) NotifyRow(a *source.Artifact, r collection.Row) {
	c.NotifyObservers(&EventRow{a, r})
}

func (c *Collection) NotifyExtractArtifactRowsEnd(a *source.Artifact, err error) {
	c.NotifyObservers(&EventExtractRowsEnd{a, err})
}

func (c *Collection) NotifySyncArtifactStart(a *source.Artifact) {
	c.NotifyObservers(&EventSyncArtifactStart{a})
}

func (c *Collection) NotifySyncArtifactEnd(a *source.Artifact, err error) {
	c.NotifyObservers(&EventSyncArtifactEnd{a, err})
}

func (c *Collection) DiscoverArtifacts() error {
	s, err := c.EnsureSource()
	if err != nil {
		return err
	}
	return s.DiscoverArtifacts()
}

func (c *Collection) Download() error {
	s, err := c.EnsureSource()
	if err != nil {
		return err
	}
	return s.Download()
}

func (c *Collection) ExtractArtifactRows(a *source.Artifact) error {
	p, err := c.EnsurePlugin()
	if err != nil {
		return err
	}

	// Limit the number of concurrent download operations. This is configurable
	// and used to prevent resource exhaustion.
	// If many downloads are queued the timeout will eventually clean them up.
	// This is set very high by default to allow large queued operations.
	ctx, cancelSemaphoreTimeout := context.WithTimeout(c.ctx, time.Duration(c.ExtractArtifactRowsConcurrencyTimeout)*time.Millisecond)
	if err := c.extractArtifactRowsConcurrencySempahore.Acquire(ctx, 1); err != nil {
		cancelSemaphoreTimeout()
		return err
	}

	go func() {
		defer cancelSemaphoreTimeout()
		defer c.extractArtifactRowsConcurrencySempahore.Release(1)

		// Set a timeout for the download operation. This should be handled
		// gracefully by the plugin.
		downloadCtx, cancelTimeout := context.WithTimeout(c.ctx, time.Duration(c.ExtractArtifactRowsTimeout)*time.Millisecond)
		defer cancelTimeout()

		// Although we want the plugin to handle timeout themselves, we also
		// need to ensure the timeout is enforced so we use a safety timeout
		// that is 10% longer than the download timeout.
		safetyTimeout := time.Duration(float64(c.ExtractArtifactRowsTimeout) * c.SafetyTimeoutMultiple)
		safetyCtx, cancelSafetyTimeout := context.WithTimeout(downloadCtx, safetyTimeout*time.Millisecond)
		defer cancelSafetyTimeout()

		c.NotifyExtractArtifactRowsStart(a)

		done := make(chan error, 1)
		go func() {
			err := p.ExtractArtifactRows(downloadCtx, a)
			done <- err
		}()

		select {
		case err := <-done:
			c.NotifyExtractArtifactRowsEnd(a, err)
		case <-safetyCtx.Done():
			c.NotifyExtractArtifactRowsEnd(a, fmt.Errorf("extract artifact rows safety timeout exceeded: %s", a.Name))
		}
	}()

	return nil
}

func (c *Collection) WriteRow(r collection.Row) error {
	if _, err := c.EnsurePlugin(); err != nil {
		return err
	}
	return c.writerManager.Write(r)
}

func (c *Collection) CloseWriters() error {
	return c.writerManager.Close()
}

func (c *Collection) SyncArtifactRows(a *source.Artifact) error {
	if _, err := c.EnsurePlugin(); err != nil {
		return err
	}

	// Limit the number of concurrent sync operations. This is configurable
	// and used to prevent resource exhaustion.
	// If many syncs are queued the timeout will eventually clean them up.
	// This is set very high by default to allow large queued operations.
	ctx, cancelSemaphoreTimeout := context.WithTimeout(c.ctx, time.Duration(c.SyncArtifactConcurrencyTimeout)*time.Millisecond)
	if err := c.syncConcurrencySempahore.Acquire(ctx, 1); err != nil {
		cancelSemaphoreTimeout()
		return err
	}

	go func() {
		defer cancelSemaphoreTimeout()
		defer c.syncConcurrencySempahore.Release(1)

		// Set a timeout for the sync operation. This should be handled
		// gracefully by the plugin.
		syncCtx, cancelSyncArtifactTimeout := context.WithTimeout(c.ctx, time.Duration(c.SyncArtifactTimeout)*time.Millisecond)
		defer cancelSyncArtifactTimeout()

		// Although we want the plugin to handle timeout themselves, we also
		// need to ensure the timeout is enforced so we use a safety timeout
		// that is 10% longer than the sync timeout.
		safetyTimeout := time.Duration(float64(c.SyncArtifactTimeout) * c.SafetyTimeoutMultiple)
		safetyCtx, cancelSafetyTimeout := context.WithTimeout(syncCtx, safetyTimeout*time.Millisecond)
		defer cancelSafetyTimeout()

		var wg sync.WaitGroup

		syncObserver := NewObserver(func(ei observer.Event) {
			// switch based on the struct of the event
			switch e := ei.(type) {
			case *EventRow:
				if e.Artifact.Name == a.Name {
					wg.Add(1)
					go func() {
						defer wg.Done()
						err := c.WriteRow(e.Row)
						if err != nil {
							// TODO - not sure how to handle this?
							panic(err)
						}
					}()
				}
			case *EventExtractRowsEnd:
				if e.Artifact.Name == a.Name {
					// Once extraction is complete, we can clear the wait for that.
					wg.Done()
				}
			}
		}).Start()

		// TODO - This is inefficient. We add observers to the main collection
		// and then every observation is sent to all of them - even though we
		// only care about observations for the given artifact.
		// Should we add observers to the artifact instead?
		c.AddObserver(syncObserver)

		defer func() {
			c.RemoveObserver(syncObserver)
			syncObserver.Stop()
		}()

		c.NotifySyncArtifactStart(a)

		// Open a wait on the extract phase
		done := make(chan error, 1)
		wg.Add(1)
		if err := c.ExtractArtifactRows(a); err != nil {
			// We didn't even start extracting, so clear the wait immediately.
			wg.Done()
			done <- err
		}

		go func() {
			wg.Wait()
			done <- nil
		}()

		select {
		case err := <-done:
			c.NotifySyncArtifactEnd(a, err)
		case <-safetyCtx.Done():
			c.NotifySyncArtifactEnd(a, fmt.Errorf("sync safety timeout exceeded"))
		}

		//fmt.Println("SyncArtifactRows", time.Now().String(), a.Name)
		//c.writerManager.PrintStats()
	}()

	return nil

}
