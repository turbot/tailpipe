package parse

import (
	"fmt"
	"github.com/turbot/pipe-fittings/v2/modconfig"
	"github.com/turbot/pipe-fittings/v2/perr"
	"github.com/turbot/tailpipe/internal/config"
	"strings"
)

// ParseResourceNameWithSubtype parses the name of a resource into type name and , where applicable, subtype.
func ParseResourceNameWithSubtype(propertyPath string) (modconfig.ResourceNameParser, error) {
	res := &ParsedResourceNameWithSubtype{Original: propertyPath}

	// valid property paths (depending on whether this resource has a subtype):
	// <resource_type>.<resource_subtype>.<resource_name>.<property path...>
	// <resource_type>.<resource_name>.<property path...>

	parts := strings.Split(propertyPath, ".")

	// does this resource type support subtypes
	hasSubtype := config.ResourceHasSubtype(parts[0])

	expectedParts := 2
	if hasSubtype {
		expectedParts = 3
	}

	if len(parts) != expectedParts {
		return nil, perr.BadRequestWithMessage(fmt.Sprintf("invalid resource name: %s - extected %d parts", propertyPath, expectedParts))
	}

	// no property path specified
	res.Type = parts[0]

	if hasSubtype {
		res.SubType = parts[1]
		res.Name = parts[2]
	} else {
		res.Name = parts[1]
	}

	return res, nil
}

// ParsedResourceNameWithSubtype represents a parsed property path for a resource with a subtype
type ParsedResourceNameWithSubtype struct {
	Type    string
	SubType string
	Name    string

	Original string
}

func (p *ParsedResourceNameWithSubtype) GetSubType() string {
	return p.SubType
}

func (p *ParsedResourceNameWithSubtype) ToFullName() string {
	//TODO implement me
	panic("implement me")
}

func (p *ParsedResourceNameWithSubtype) ToFullNameWithMod(mod string) string {
	//TODO implement me
	panic("implement me")
}

func (p *ParsedResourceNameWithSubtype) GetMod() string {
	return ""
}

func (p *ParsedResourceNameWithSubtype) GetItemType() string {
	return p.Type
}

func (p *ParsedResourceNameWithSubtype) GetName() string {
	return p.Name
}

func (p *ParsedResourceNameWithSubtype) ToResourceName() string {
	if p.SubType == "" {
		return fmt.Sprintf("%s.%s", p.Type, p.Name)
	}
	return fmt.Sprintf("%s.%s.%s", p.Type, p.SubType, p.Name)
}

func (p *ParsedResourceNameWithSubtype) String() string {
	return p.Original
}
