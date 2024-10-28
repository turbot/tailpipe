package parse

import (
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/cty_helpers"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/pipe-fittings/parse"
	"github.com/turbot/pipe-fittings/schema"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/zclconf/go-cty/cty"
)

type ConfigParseContext struct {
	parse.ParseContext

	// resourceValues is keyed  by resource type, then by resource nam
	resourceValues map[string]map[string]cty.Value

	// map of all resources, keyed by full name
	resourceMap map[string]modconfig.HclResource
}

func (c *ConfigParseContext) GetResource(parsedName *modconfig.ParsedResourceName) (resource modconfig.HclResource, found bool) {
	resource, ok := c.resourceMap[parsedName.ToResourceName()]
	return resource, ok
}

func NewConfigParseContext(rootEvalPath string) *ConfigParseContext {
	parseContext := parse.NewParseContext(rootEvalPath)
	c := &ConfigParseContext{
		ParseContext:   parseContext,
		resourceValues: make(map[string]map[string]cty.Value),
		resourceMap:    make(map[string]modconfig.HclResource),
	}

	// only load partition blocks - we parse connections, and workspoaces separately
	c.SetBlockTypes(schema.BlockTypePartition)

	//override ResourceNameFromDependencyFunc to use a version
	// which uses the local ParsedPropertyPath type
	c.ResourceNameFromDependencyFunc = resourceNameFromDependency
	c.buildEvalContext()

	return c
}

// AddResource stores this resource as a variable to be added to the eval context.
func (c *ConfigParseContext) AddResource(resource modconfig.HclResource) hcl.Diagnostics {
	// TODO handle 3 part names
	name := resource.Name()
	ctyVal, err := cty_helpers.GetCtyValue(resource)
	if err != nil {
		return hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  fmt.Sprintf("failed to convert resource '%s' to its cty value", name),
			Detail:   err.Error(),
			Subject:  resource.GetDeclRange(),
		}}
	}

	resourceType := resource.BlockType()
	mapForType := c.resourceValues[resourceType]
	if mapForType == nil {
		mapForType = make(map[string]cty.Value)
	}
	// ad value to map
	mapForType[resource.GetShortName()] = ctyVal
	// write back
	c.resourceValues[resourceType] = mapForType

	// remove this resource from unparsed blocks
	delete(c.UnresolvedBlocks, name)

	// rebuild eval context
	c.buildEvalContext()

	return nil
}

func (c *ConfigParseContext) buildEvalContext() {
	// rebuild the eval context
	vars := map[string]cty.Value{}
	for resourceType, valueMap := range c.resourceValues {
		vars[resourceType] = cty.ObjectVal(valueMap)
	}
	c.ParseContext.BuildEvalContext(vars)

}

// AddDependencies is called when a block could not be resolved as it has dependencies
// 1) store block as unresolved
// 2) add dependencies to our tree of dependencies
// NOTE: this overrides  ParseContext.AddDependencies to allwo us to override ParseResourcePropertyPath
// to handle resource type labels
func (c *ConfigParseContext) AddDependencies(block *hcl.Block, name string, dependencies map[string]*modconfig.ResourceDependency) hcl.Diagnostics {
	var diags hcl.Diagnostics

	if c.UnresolvedBlocks[name] != nil {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  fmt.Sprintf("duplicate unresolved block name '%s'", name),
			Detail:   fmt.Sprintf("block '%s' already exists. This could mean that there are unresolved duplicate resources,", name),
			Subject:  &block.DefRange,
		})
		return diags
	}

	// store unresolved block
	c.UnresolvedBlocks[name] = parse.NewUnresolvedBlock(block, name, dependencies)

	// store dependency in tree - d
	if !c.DependencyGraph.ContainsNode(name) {
		c.DependencyGraph.AddNode(name)
	}
	// add root dependency
	if err := c.DependencyGraph.AddEdge(parse.RootDependencyNode, name); err != nil {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "failed to add root dependency to graph",
			Detail:   err.Error(),
			Subject:  hclhelpers.BlockRangePointer(block),
		})
	}

	for _, dep := range dependencies {
		// each dependency object may have multiple traversals
		for _, t := range dep.Traversals {
			parsedPropertyPath, err := config.ParseResourcePropertyPath(hclhelpers.TraversalAsString(t))

			if err != nil {
				diags = append(diags, &hcl.Diagnostic{
					Severity: hcl.DiagError,
					Summary:  "failed to parse dependency",
					Detail:   err.Error(),
					Subject:  hclhelpers.BlockRangePointer(block),
				})
				continue
			}
			if parsedPropertyPath == nil {
				continue
			}

			// 'd' may be a property path - when storing dependencies we only care about the resource names
			dependencyResourceName := parsedPropertyPath.ToResourceName()
			if !c.DependencyGraph.ContainsNode(dependencyResourceName) {
				c.DependencyGraph.AddNode(dependencyResourceName)
			}
			if err := c.DependencyGraph.AddEdge(name, dependencyResourceName); err != nil {
				diags = append(diags, &hcl.Diagnostic{
					Severity: hcl.DiagError,
					Summary:  "failed to add dependency to graph",
					Detail:   err.Error(),
					Subject:  hclhelpers.BlockRangePointer(block),
				})
			}
		}
	}
	return diags
}

// overriden resourceNameFromDependency func
func resourceNameFromDependency(propertyPath string) (string, error) {
	parsedPropertyPath, err := config.ParseResourcePropertyPath(propertyPath)

	if err != nil {
		return "", err
	}
	if parsedPropertyPath == nil {
		return "", nil
	}

	// 'd' may be a property path - when storing dependencies we only care about the resource names
	dependencyResourceName := parsedPropertyPath.ToResourceName()
	return dependencyResourceName, nil
}
