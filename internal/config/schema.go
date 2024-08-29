package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/go-kit/helpers"
)

const (
	BlockTypePartition = "partition"
	//BlockTypeCredential  = "credential"
	//BlockTypeFilter      = "filter"
	BlockTypeSource = "source"
	//BlockTypeDestination = "destination"
)

// ConfigBlockSchema is the schema for the supported top level blocks in the config file
var ConfigBlockSchema = &hcl.BodySchema{
	Attributes: []hcl.AttributeSchema{},
	Blocks: []hcl.BlockHeaderSchema{
		{
			Type:       BlockTypePartition,
			LabelNames: []string{"type", "name"},
		},
		//{
		//	Type:       BlockTypeCredential,
		//	LabelNames: []string{"type", "name"},
		//},
		//{
		//	Type:       BlockTypeFilter,
		//	LabelNames: []string{"name"},
		//},
		//
		//{
		//	Type:       BlockTypeSource,
		//	LabelNames: []string{"type", "name"},
		//},
		//{
		//	Type:       BlockTypeDestination,
		//	LabelNames: []string{"type", "name"},
		//},
	},
}

var validResourceItemTypes = []string{
	BlockTypePartition,
	//BlockTypeCredential,
	//BlockTypeFilter,
	BlockTypeSource,
	//BlockTypeDestination,
}

func IsValidResourceItemType(blockType string) bool {
	return helpers.StringSliceContains(validResourceItemTypes, blockType)
}
