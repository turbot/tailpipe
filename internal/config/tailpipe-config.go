package config

import (
	"fmt"
	"github.com/turbot/pipe-fittings/modconfig"
)

type TailpipeConfig struct {
	// map of collections, keyed by unqualified name (<collection_type>.<collection_name>)
	Collections map[string]*Collection
}

func NewTailpipeConfig() *TailpipeConfig {
	return &TailpipeConfig{
		Collections: make(map[string]*Collection),
	}
}
func (c *TailpipeConfig) Add(resource modconfig.HclResource) error {
	switch t := resource.(type) {
	case *Collection:
		c.Collections[t.GetUnqualifiedName()] = t
		return nil
	default:
		return fmt.Errorf("unsupported resource type %T", t)
	}
}
