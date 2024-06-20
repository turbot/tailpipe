package config

import (
	"github.com/turbot/pipe-fittings/modconfig"
)

type TailpipeConfig struct {
	Sources      map[string]*Source
	Destinations map[string]*Destination
	Collections  map[string]*Collection
	//Filters      map[string]*Filter
	Credentials map[string]*Credential
}

func NewTailpipeConfig() *TailpipeConfig {
	return &TailpipeConfig{
		Sources:      make(map[string]*Source),
		Destinations: make(map[string]*Destination),
		Collections:  make(map[string]*Collection),
		//Filters:      make(map[string]*Filter),
		Credentials: make(map[string]*Credential),
	}
}
func (c *TailpipeConfig) Add(resource modconfig.HclResource) {
	switch t := resource.(type) {
	case *Source:
		c.Sources[t.Name()] = t
	case *Destination:
		c.Destinations[t.Name()] = t
	case *Collection:
		c.Collections[t.Name()] = t
	case *Credential:
		c.Credentials[t.Name()] = t
	}
}
