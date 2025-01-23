package config

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/pipe-fittings/plugin"
	"github.com/turbot/pipe-fittings/schema"
	"github.com/turbot/tailpipe/internal/constants"
)

func init() {
	registerResourceWithSubType(schema.BlockTypePartition)
}

type Partition struct {
	modconfig.HclResourceImpl

	// the name of the table this partition is for - this is the first label in the partition block
	TableName string

	// if the partition of for a custom table, this will be set to the custom table config
	CustomTable *Table `cty:"table"`

	// Plugin used for this partition
	Plugin *plugin.Plugin `cty:"-"`

	// Source of the data for this partition
	Source Source `cty:"source"`

	// any partition-type specific config data for the partition
	Config []byte `cty:"config"`
	// the config location
	ConfigRange hclhelpers.Range `cty:"config_range"`
	// an option filter in the format of a SQL where clause
	Filter string
}

func NewPartition(block *hcl.Block, fullName string) (modconfig.HclResource, hcl.Diagnostics) {
	if len(block.Labels) < 2 {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "'partition' block requires 2 labels, 'type' and 'name'",
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}
	c := &Partition{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fullName),
		TableName:       block.Labels[0],
	}

	// NOTE: as tailpipe does not have the concept of mods, the full name is partition.<type>.<name> and
	// the unqualified name is the <type>.<name>
	c.UnqualifiedName = fmt.Sprintf("%s.%s", c.TableName, c.ShortName)
	return c, nil
}

func (c *Partition) SetConfigHcl(u *HclBytes) {
	if u == nil {
		return
	}
	c.Config = u.Hcl
	c.ConfigRange = u.Range
}

func (c *Partition) InferPluginName() string {
	if c.CustomTable != nil {
		return constants.CorePluginName
	}
	// otherwise just use the first segment of the table name
	return strings.Split(c.TableName, "_")[0]
}

func (c *Partition) AddFilter(filter string) {
	if c.Filter == "" {
		c.Filter = filter
	} else {
		c.Filter += " AND " + filter
	}
}

func (c *Partition) CollectionStatePath(collectionDir string) string {
	// return the path to the collection state file
	return filepath.Join(collectionDir, fmt.Sprintf("collection_state_%s_%s.json", c.TableName, c.ShortName))
}
