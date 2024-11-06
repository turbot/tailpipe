package config

import (
	"fmt"
	"strings"

	"github.com/turbot/pipe-fittings/perr"
)

// TODO K move to pipe-fittings and think about combining with existing ParsedResourceName

// ParsedPropertyPath represents a parsed property path for a resource with a subtype
type ParsedPropertyPath struct {
	ItemType     string
	ItemSubType  string
	Name         string
	PropertyPath []string
	Original     string
}

func (p *ParsedPropertyPath) PropertyPathString() string {
	return strings.Join(p.PropertyPath, ".")
}

func (p *ParsedPropertyPath) ToResourceName() string {
	return BuildResourceName(p.ItemType, p.ItemSubType, p.Name)
}

func (p *ParsedPropertyPath) String() string {
	return p.Original
}

func ParseResourcePropertyPath(propertyPath string) (*ParsedPropertyPath, error) {
	res := &ParsedPropertyPath{Original: propertyPath}

	// valid property paths:
	// <resource_type>.<resource_subtype>.<resource_name>.<property path...>

	parts := strings.Split(propertyPath, ".")
	if len(parts) < 3 {
		return nil, perr.BadRequestWithMessage("invalid property path: " + propertyPath)
	}

	// no property path specified
	res.ItemType = parts[0]
	res.ItemSubType = parts[1]
	res.Name = parts[2]
	// if a property path is set, add it
	if len(parts) > 3 {
		res.PropertyPath = parts[3:]
	}

	return res, nil
}

// TODO do we need this split out?
func BuildResourceName(resourceType, resourceSubType, name string) string {
	return fmt.Sprintf("%s.%s.%s", resourceType, resourceSubType, name)
}
