package config

import (
	"fmt"
	"github.com/turbot/pipe-fittings/modconfig"
)

type TailpipeConfig struct {
	// map of partitions, keyed by unqualified name (<partition_type>.<partition_name>)
	Partitions map[string]*Partition
}

func NewTailpipeConfig() *TailpipeConfig {
	return &TailpipeConfig{
		Partitions: make(map[string]*Partition),
	}
}
func (c *TailpipeConfig) Add(resource modconfig.HclResource) error {
	switch t := resource.(type) {
	case *Partition:
		c.Partitions[t.GetUnqualifiedName()] = t
		return nil
	default:
		return fmt.Errorf("unsupported resource type %T", t)
	}
}
