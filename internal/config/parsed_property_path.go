package config

import (
	"fmt"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/perr"
)

func PropertyPathFromExpression(expr hcl.Expression) (bool, *ParsedPropertyPath, error) {
	var propertyPathStr string
	var isArray bool

dep_loop:
	for {
		switch e := expr.(type) {
		case *hclsyntax.ScopeTraversalExpr:
			propertyPathStr = hclhelpers.TraversalAsString(e.Traversal)
			break dep_loop
		case *hclsyntax.SplatExpr:
			root := hclhelpers.TraversalAsString(e.Source.(*hclsyntax.ScopeTraversalExpr).Traversal)
			var suffix string
			// if there is a property path, add it
			if each, ok := e.Each.(*hclsyntax.RelativeTraversalExpr); ok {
				suffix = fmt.Sprintf(".%s", hclhelpers.TraversalAsString(each.Traversal))
			}
			propertyPathStr = fmt.Sprintf("%s.*%s", root, suffix)
			break dep_loop
		case *hclsyntax.TupleConsExpr:
			// TACTICAL
			// handle the case where an arg value is given as a runtime dependency inside an array, for example
			// arns = [input.arn]
			// this is a common pattern where a runtime depdency gives a scalar value, but an array is needed for the arg
			// NOTE: this code only supports a SINGLE item in the array
			if len(e.Exprs) != 1 {
				return false, nil, fmt.Errorf("unsupported runtime dependency expression - only a single runtime dependency item may be wrapped in an array")
			}
			isArray = true
			expr = e.Exprs[0]
			// fall through to rerun loop with updated expr
		default:
			// unhandled expression type
			return false, nil, fmt.Errorf("unexpected runtime dependency expression type")
		}
	}

	propertyPath, err := ParseResourcePropertyPath(propertyPathStr)
	if err != nil {
		return false, nil, err
	}
	return isArray, propertyPath, nil
}

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

func (p *ParsedPropertyPath) ToParsedResourceName() *ParsedResourceName {
	return &ParsedResourceName{
		ItemType:    p.ItemType,
		ItemSubType: p.ItemSubType,
		Name:        p.Name,
	}
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

	if !IsValidResourceItemType(res.ItemType) {
		return nil, perr.BadRequestWithMessage("invalid resource item type passed to ParseResourcePropertyPath: " + propertyPath)
	}

	return res, nil
}
