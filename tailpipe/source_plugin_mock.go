package tailpipe

import (
	"context"
	"sync"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/source"
)

func mockCustomPluginArtifactInfos() map[string]*source.ArtifactInfo {
	infos := make(map[string]*source.ArtifactInfo)
	for _, name := range mockCustomPluginArtifacts() {
		infos[name.Name] = &name.ArtifactInfo
	}
	return infos
}

func mockCustomPluginArtifacts() map[string]*source.Artifact {
	return map[string]*source.Artifact{
		"a.jsonl": {ArtifactInfo: source.ArtifactInfo{Name: "a.jsonl"}, Data: []byte(`{"artifact": "a.jsonl"}`)},
		"b.jsonl": {ArtifactInfo: source.ArtifactInfo{Name: "b.jsonl"}, Data: []byte(`{"artifact": "b.jsonl"}`)},
		"c.jsonl": {ArtifactInfo: source.ArtifactInfo{Name: "c.jsonl"}, Data: []byte(`{"artifact": "c.jsonl"}`)},
	}
}

type MockSourcePlugin struct {
	ctx context.Context

	// observers is a list of observers that will be notified of events.
	observers      []source.SourceObserver
	observersMutex sync.RWMutex
}

func (s *MockSourcePlugin) AddObserver(observer source.SourceObserver) {
	s.observersMutex.Lock()
	defer s.observersMutex.Unlock()
	s.observers = append(s.observers, observer)
}

func (s *MockSourcePlugin) RemoveObserver(observer source.SourceObserver) {
	s.observersMutex.Lock()
	defer s.observersMutex.Unlock()
	for i, o := range s.observers {
		if o == observer {
			s.observers = append(s.observers[:i], s.observers[i+1:]...)
			break
		}
	}
}

func (p *MockSourcePlugin) Identifier() string {
	return "stub"
}

func (p *MockSourcePlugin) Init(ctx context.Context) error {
	p.ctx = ctx
	return nil
}

func (p *MockSourcePlugin) Context() context.Context {
	return p.ctx
}

func (p *MockSourcePlugin) LoadConfig(raw []byte) error {
	return nil
}

func (p *MockSourcePlugin) ValidateConfig() error {
	return nil
}

func (p *MockSourcePlugin) DiscoverArtifacts(ctx context.Context) error {
	total := int64(len(mockCustomPluginArtifactInfos()))
	i := int64(1)
	// Short sleep to simulate discovery time. This helps with various
	// tests that check timeouts.
	time.Sleep(5 * time.Millisecond)
	for _, ai := range mockCustomPluginArtifactInfos() {
		for _, o := range p.observers {
			o.NotifyArtifactDiscovered(ai)
			o.NotifyDiscoverArtifactsProgress(i, total)
		}
		i++
	}
	return nil
}

func (p *MockSourcePlugin) DownloadArtifact(ctx context.Context, ai *source.ArtifactInfo) error {
	for _, o := range p.observers {
		o.NotifyDownloadArtifactProgress(ai, 3, 4)
		o.NotifyArtifactDownloaded(mockCustomPluginArtifacts()[ai.Name])
	}
	return nil
}
