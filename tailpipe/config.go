package tailpipe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"plugin"
	"sync"

	"github.com/turbot/tailpipe-plugin-sdk/collection"
	"github.com/turbot/tailpipe-plugin-sdk/observer"
	sdkplugin "github.com/turbot/tailpipe-plugin-sdk/plugin"
	"github.com/turbot/tailpipe-plugin-sdk/source"
)

var (
	globalConfigInstance *Config
)

type PluginPath string

type Config struct {
	Path string
	Mock bool

	Destinations      map[string]*Destination      `json:"destinations"`
	Collections       map[string]*Collection       `json:"collections"`
	Schemas           map[string]*Schema           `json:"schemas"`
	Sources           map[string]*Source           `json:"sources"`
	SourcePlugins     map[string]source.Plugin     `json:"source_plugins"`
	CollectionPlugins map[string]collection.Plugin `json:"collection_plugins"`

	ctx context.Context

	// observers is a list of observers that will be notified of events.
	observers      []observer.ObserverInterface
	observersMutex sync.RWMutex
}

type ConfigOption func(*Config) error

func InitConfig(ctx context.Context, opts ...ConfigOption) (*Config, error) {
	var err error
	globalConfigInstance, err = newConfig(ctx, opts...)
	return globalConfigInstance, err
}

func GetConfig() *Config {
	return globalConfigInstance
}

func newConfig(ctx context.Context, opts ...ConfigOption) (*Config, error) {

	c := &Config{
		ctx:               ctx,
		Destinations:      make(map[string]*Destination),
		Collections:       make(map[string]*Collection),
		Sources:           make(map[string]*Source),
		Schemas:           make(map[string]*Schema),
		SourcePlugins:     make(map[string]source.Plugin),
		CollectionPlugins: make(map[string]collection.Plugin),
	}

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	if c.Path == "" {
		c.setDefaultPath()
	}

	if err := c.LoadBytes([]byte(`{}`)); err != nil {
		return nil, err
	}

	return c, nil
}

func WithMock() ConfigOption {
	return func(c *Config) error {
		c.Mock = true
		return nil
	}
}

func WithConfigObservers(observers ...observer.ObserverInterface) ConfigOption {
	return func(c *Config) error {
		c.observers = append(c.observers, observers...)
		return nil
	}
}

func (c *Config) AddObserver(o observer.ObserverInterface) {
	c.observersMutex.Lock()
	defer c.observersMutex.Unlock()
	c.observers = append(c.observers, o)
}

func (c *Config) RemoveObserver(observer observer.ObserverInterface) {
	c.observersMutex.Lock()
	defer c.observersMutex.Unlock()
	for i, o := range c.observers {
		if o == observer {
			c.observers = append(c.observers[:i], c.observers[i+1:]...)
			break
		}
	}
}

func (c *Config) NotifyObservers(event observer.Event) {
	c.observersMutex.RLock()
	defer c.observersMutex.RUnlock()
	for _, o := range c.observers {
		o.Notify(event)
	}
}

func (c *Config) Load() error {
	// Check if the config file path is set
	if c.Path == "" {
		return fmt.Errorf("config file path is not set")
	}

	// Open the config file
	file, err := os.Open(c.Path)
	if err != nil {
		return fmt.Errorf("opening config file: %w", err)
	}
	defer file.Close()

	// Read the file content
	bytes, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("reading config file: %w", err)
	}

	return c.LoadBytes(bytes)
}

func (c *Config) LoadBytes(b []byte) error {
	if err := json.Unmarshal(b, c); err != nil {
		return fmt.Errorf("parsing config JSON: %w", err)
	}

	/*
		for _, sp := range c.SourcePlugins {
			if err := sp.Init(c.ctx); err != nil {
				return err
			}
		}
	*/
	if c.Mock && c.SourcePlugins["mock"] == nil {
		c.SourcePlugins["mock"] = &MockSourcePlugin{}
	}

	/*
		for _, cp := range c.CollectionPlugins {
			if err := cp.Init(c.ctx); err != nil {
				return err
			}
		}
	*/
	if c.Mock && c.CollectionPlugins["mock"] == nil {
		c.CollectionPlugins["mock"] = &MockCollectionPlugin{}
	}

	for _, d := range c.Destinations {
		if err := d.Init(c.ctx); err != nil {
			return err
		}
	}
	if c.Destinations["default"] == nil {
		defaultDestination, err := NewDestination(c.ctx, "default")
		if err != nil {
			return err
		}
		c.Destinations["default"] = defaultDestination
	}

	for _, s := range c.Sources {
		if err := s.Init(c.ctx); err != nil {
			return err
		}
	}
	if c.Mock && c.Sources["mock"] == nil {
		s, err := NewSource(c.ctx, "mock", "mock")
		if err != nil {
			return err
		}
		c.Sources["mock"] = s
	}

	for _, coll := range c.Collections {
		if err := coll.Init(c.ctx); err != nil {
			return err
		}
	}

	for _, s := range c.Schemas {
		if err := s.Init(c.ctx); err != nil {
			return err
		}
	}

	return nil
}

func (c *Config) setDefaultPath() error {
	p := os.Getenv("TAILPIPE_PATH")
	if p == "" {
		homePath, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		p = filepath.Join(homePath, ".tailpipe")
	}
	c.SetPath(p)
	return nil
}

func (c *Config) SetPath(s string) {
	c.Path = s
	c.NotifyObservers(&EventPathChange{s})
}

// findPlugins scans the given directory for .so files and returns their paths
func findPlugins(dir string) ([]string, error) {
	var plugins []string
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".so" {
			plugins = append(plugins, filepath.Join(dir, f.Name()))
		}
	}
	return plugins, nil
}

// loadPlugin loads a single plugin and checks its type
func loadPlugin(ctx context.Context, pluginPath string) (sdkplugin.Plugin, error) {
	plug, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, err
	}

	symPlugin, err := plug.Lookup("Plugin")
	if err != nil {
		return nil, err
	}

	switch p := symPlugin.(type) {
	case sdkplugin.Plugin:
		p.Init(context.WithValue(ctx, PluginPath("path"), pluginPath))
		return p, nil
	default:
		return nil, fmt.Errorf("plugin is not a tailpipe plugin: %s", pluginPath)
	}
}

func CloneSourcePlugin(sp source.Plugin) (source.Plugin, error) {

	path := sp.Context().Value(PluginPath("path")).(string)

	plug, err := plugin.Open(path)
	if err != nil {
		return nil, err
	}

	symPlugin, err := plug.Lookup("Plugin")
	if err != nil {
		return nil, err
	}

	if p, ok := symPlugin.(sdkplugin.Plugin); ok {
		s := p.Sources()[sp.Identifier()]
		if s == nil {
			return nil, fmt.Errorf("source plugin not found: %s", sp.Identifier())
		}
		s.Init(p.Context())
		return s, nil
	}

	return nil, fmt.Errorf("plugin is not a source plugin")
}

func CloneCollectionPlugin(cp collection.Plugin) (collection.Plugin, error) {

	path := cp.Context().Value(PluginPath("path")).(string)

	plug, err := plugin.Open(path)
	if err != nil {
		return nil, err
	}

	symPlugin, err := plug.Lookup("Plugin")
	if err != nil {
		return nil, err
	}

	if p, ok := symPlugin.(sdkplugin.Plugin); ok {
		c := p.Collections()[cp.Identifier()]
		if c == nil {
			return nil, fmt.Errorf("collection plugin not found: %s", cp.Identifier())
		}
		c.Init(cp.Context())
		return c, nil
	}

	return nil, fmt.Errorf("plugin is not a collection plugin")
}

func (c *Config) LoadPlugins() error {

	dir := filepath.Join(c.Path, "plugins")

	pluginPaths, err := findPlugins(dir)
	if err != nil {
		return err
	}

	for _, path := range pluginPaths {
		p, err := loadPlugin(c.ctx, path)
		if err != nil {
			return err
		}
		for id, s := range p.Sources() {
			s.Init(context.WithValue(c.ctx, PluginPath("path"), path))
			c.SourcePlugins[id] = s
		}
		for _, coll := range p.Collections() {
			coll.Init(context.WithValue(c.ctx, PluginPath("path"), path))
			c.CollectionPlugins[coll.Identifier()] = coll
		}
	}

	return nil
}

// CollectionsMatching will return a map of the collections that
// match the given list of name globs.
func (c *Config) CollectionsMatching(globs []string) map[string]*Collection {
	matchingCollections := make(map[string]*Collection)
	for _, coll := range c.Collections {
		for _, glob := range globs {
			match, _ := filepath.Match(glob, coll.Name)
			if match {
				matchingCollections[coll.Name] = coll
				break
			}
		}
	}
	return matchingCollections
}

func (c *Config) Destination(name string) *Destination {
	return c.Destinations[name]
}

func (c *Config) Source(name string) *Source {
	return c.Sources[name]
}

func (c *Config) Collection(name string) *Collection {
	return c.Collections[name]
}

func (c *Config) CollectionPlugin(name string) collection.Plugin {
	return c.CollectionPlugins[name]
}

func (c *Config) SourcePlugin(name string) source.Plugin {
	return c.SourcePlugins[name]
}
