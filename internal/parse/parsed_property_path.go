package parse

import (
	"fmt"
	"github.com/turbot/pipe-fittings/v2/perr"
	"github.com/turbot/tailpipe/internal/config"
	"strings"
)

// ParsedPropertyPath represents a parsed property path for a resource with a subtype
type ParsedPropertyPath struct {
	Type         string
	SubType      string
	Name         string
	PropertyPath []string
	Original     string
}

func ParseResourcePropertyPath(propertyPath string) (*ParsedPropertyPath, error) {
	res := &ParsedPropertyPath{Original: propertyPath}

	// valid property paths (depending on whether this resource has a subtype):
	// <resource_type>.<resource_subtype>.<resource_name>.<property path...>
	// <resource_type>.<resource_name>.<property path...>

	parts := strings.Split(propertyPath, ".")

	// does this resource type support subtypes
	hasSubtype := config.ResourceHasSubtype(parts[0])

	minParts := 2
	if hasSubtype {
		minParts = 3
	}

	if len(parts) < minParts {
		return nil, perr.BadRequestWithMessage(fmt.Sprintf("invalid resource name: %s - at least %d parts required", propertyPath, minParts))
	}

	// no property path specified
	res.Type = parts[0]

	if hasSubtype {
		res.SubType = parts[1]
		res.Name = parts[2]
	} else {
		res.Name = parts[1]
	}
	// if a property path is set, add it
	if len(parts) > minParts {
		res.PropertyPath = parts[minParts:]
	}

	return res, nil
}

func (p *ParsedPropertyPath) PropertyPathString() string {
	return strings.Join(p.PropertyPath, ".")
}

func (p *ParsedPropertyPath) ToResourceName() string {
	if p.SubType == "" {
		return fmt.Sprintf("%s.%s", p.Type, p.Name)
	}
	return fmt.Sprintf("%s.%s.%s", p.Type, p.SubType, p.Name)
}

func (p *ParsedPropertyPath) String() string {
	return p.Original
}
