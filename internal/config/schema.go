package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/go-kit/helpers"
)

const (
	BlockTypeCollection  = "collection"
	BlockTypeCredential  = "credential"
	BlockTypeFilter      = "filter"
	BlockTypeSource      = "source"
	BlockTypeDestination = "destination"
)

var ConfigBlockSchema = &hcl.BodySchema{
	Attributes: []hcl.AttributeSchema{},
	Blocks: []hcl.BlockHeaderSchema{
		{
			Type:       BlockTypeCollection,
			LabelNames: []string{"type", "name"},
		},
		{
			Type:       BlockTypeCredential,
			LabelNames: []string{"type", "name"},
		},
		{
			Type:       BlockTypeFilter,
			LabelNames: []string{"name"},
		},

		{
			Type:       BlockTypeSource,
			LabelNames: []string{"type", "name"},
		},
		{
			Type:       BlockTypeDestination,
			LabelNames: []string{"type", "name"},
		},
	},
}

var validResourceItemTypes = []string{
	BlockTypeCollection,
	BlockTypeCredential,
	BlockTypeFilter,
	BlockTypeSource,
	BlockTypeDestination,
}

func IsValidResourceItemType(blockType string) bool {
	return helpers.StringSliceContains(validResourceItemTypes, blockType)
}
