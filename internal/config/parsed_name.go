package config

import (
	"fmt"
	"strings"

	"github.com/turbot/pipe-fittings/perr"
)

type ParsedResourceName struct {
	ItemType    string
	ItemSubType string
	Name        string
}

// TODO do all resources have a subtype???
func ParseResourceName(fullName string) (res *ParsedResourceName, err error) {
	res = &ParsedResourceName{}
	if fullName == "" {
		return res, nil
	}

	// valid resource name:
	// <resource_type>.<resource_subtype>.<resource_name>

	parts := strings.Split(fullName, ".")
	if len(parts) != 3 {
		return nil, perr.BadRequestWithMessage("invalid resource name: " + fullName)
	}

	// no property path specified
	res.ItemType = parts[0]
	res.ItemSubType = parts[1]
	res.Name = parts[2]

	return res, nil
}

func (p *ParsedResourceName) ToResourceName() string {
	return BuildResourceName(p.ItemType, p.ItemSubType, p.Name)
}

// TODO do we need this split out?
func BuildResourceName(resourceType, resourceSubType, name string) string {
	return fmt.Sprintf("%s.%s.%s", resourceType, resourceSubType, name)
}
