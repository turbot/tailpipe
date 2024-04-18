package tailpipe

import (
	"context"
	"sync"

	"github.com/turbot/tailpipe-plugin-sdk/collection"
	"github.com/turbot/tailpipe-plugin-sdk/source"
)

type MockCollectionPlugin struct {
	ctx context.Context

	// observers is a list of observers that will be notified of events.
	observers      []collection.CollectionObserver
	observersMutex sync.RWMutex
}

func (s *MockCollectionPlugin) AddObserver(observer collection.CollectionObserver) {
	s.observersMutex.Lock()
	defer s.observersMutex.Unlock()
	s.observers = append(s.observers, observer)
}

func (s *MockCollectionPlugin) RemoveObserver(observer collection.CollectionObserver) {
	s.observersMutex.Lock()
	defer s.observersMutex.Unlock()
	for i, o := range s.observers {
		if o == observer {
			s.observers = append(s.observers[:i], s.observers[i+1:]...)
			break
		}
	}
}

func (p *MockCollectionPlugin) Identifier() string {
	return "stub"
}

func (p *MockCollectionPlugin) Init(ctx context.Context) error {
	p.ctx = ctx
	return nil
}

func (p *MockCollectionPlugin) Context() context.Context {
	return p.ctx
}

func (p *MockCollectionPlugin) LoadConfig(raw []byte) error {
	return nil
}

func (p *MockCollectionPlugin) ValidateConfig() error {
	return nil
}

func (p *MockCollectionPlugin) ExtractArtifactRows(ctx context.Context, a *source.Artifact) error {
	return nil
}

func (p *MockCollectionPlugin) Schema() collection.Row {
	return nil
}
