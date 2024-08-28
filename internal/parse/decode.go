package parse

import (
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/pipe-fittings/parse"
	"github.com/turbot/tailpipe/internal/config"
	"golang.org/x/exp/maps"
)

func decode(parseCtx *ConfigParseContext) (*config.TailpipeConfig, hcl.Diagnostics) {
	var diags hcl.Diagnostics
	blocksToDecode, err := parseCtx.BlocksToDecode()
	// build list of blocks to decode
	if err != nil {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "failed to determine required dependency order",
			Detail:   err.Error()})
		return nil, diags
	}

	// now clear dependencies from run context - they will be rebuilt
	parseCtx.ClearDependencies()

	var config = config.NewTailpipeConfig()
	for _, block := range blocksToDecode {
		resource, res := decodeBlock(block, parseCtx)
		diags = append(diags, res.Diags...)
		if !res.Success() || resource == nil {
			continue
		}
		config.Add(resource)

	}
	return config, diags
}

func decodeBlock(block *hcl.Block, parseCtx *ConfigParseContext) (modconfig.HclResource, *parse.DecodeResult) {
	var resource modconfig.HclResource
	var res = parse.NewDecodeResult()

	// check name is valid
	diags := parse.ValidateName(block)
	if diags.HasErrors() {
		res.AddDiags(diags)
		return nil, res
	}

	// now do the actual decode
	resource, res = decodeResource(block, parseCtx)

	// Note that an interface value that holds a nil concrete value is itself non-nil.
	if !helpers.IsNil(resource) {
		// handle the result
		// - if there are dependencies, add to run context
		handleDecodeResult(resource, res, block, parseCtx)
	}

	return resource, res
}

// generic decode function for any resource we do not have custom decode logic for
func decodeResource(block *hcl.Block, parseCtx *ConfigParseContext) (modconfig.HclResource, *parse.DecodeResult) {
	res := parse.NewDecodeResult()
	// get shell resource
	resource, diags := resourceForBlock(block)
	if diags.HasErrors() {
		return nil, res
	}

	switch block.Type {
	case config.BlockTypeCollection:
		return decodeCollection(block, parseCtx, resource)
	}

	diags = parse.DecodeHclBody(block.Body, parseCtx.EvalCtx, parseCtx, resource)
	//// if this resource supports unknown hcl, handle it
	//diags = handleUnknownHcl(block, parseCtx, resource, diags)

	res.HandleDecodeDiags(diags)
	return resource, res
}

func decodeCollection(block *hcl.Block, parseCtx *ConfigParseContext, resource modconfig.HclResource) (modconfig.HclResource, *parse.DecodeResult) {
	res := parse.NewDecodeResult()

	target := resource.(*config.Collection)
	syntaxBody := block.Body.(*hclsyntax.Body)

	attrs := syntaxBody.Attributes

	var propertyNames = map[string]struct{}{"plugin": {}, "source": {}}

	var unknownAttrs []*hcl.Attribute
	for name, attr := range attrs {

		if _, ok := propertyNames[name]; ok {
			// try to evaluate expression
			val, diags := attr.Expr.Value(parseCtx.EvalCtx)
			if diags.HasErrors() {
				continue
			}
			switch name {
			case "plugin":
				target.Plugin = val.AsString()
			}
		} else {
			unknownAttrs = append(unknownAttrs, attr.AsHCLAttribute())
		}
	}

	unknown, diags := handleUnknownAttributes(block, parseCtx, unknownAttrs)
	// now set unknown hcl
	target.SetUnknownHcl(unknown)

	blocks := syntaxBody.Blocks
	for _, block := range blocks {
		switch block.Type {
		case "source":
			// decode source block
			source, diags := decodeSource(block, parseCtx)
			res.HandleDecodeDiags(diags)
			if !diags.HasErrors() {
				target.Source = *source
			}
		default:
			diagnostics := hcl.Diagnostics{&hcl.Diagnostic{
				Severity: hcl.DiagError,
				Summary:  fmt.Sprintf("unexpected block type %s", block.Type),
				Subject:  hclhelpers.BlockRangePointer(block.AsHCLBlock()),
			},
			}
			res.HandleDecodeDiags(diagnostics)
		}
	}

	res.HandleDecodeDiags(diags)
	return target, res
}

func handleUnknownAttributes(block *hcl.Block, parseCtx *ConfigParseContext, unknownAttrs []*hcl.Attribute) (*config.UnknownHcl, hcl.Diagnostics) {
	var diags hcl.Diagnostics
	unknown := &config.UnknownHcl{}
	for _, attr := range unknownAttrs {
		// call sdk handleUnsupportedArgDiags to extract unknown hcl
		hclBytes := parseCtx.FileData[block.DefRange.Filename]

		// extract the unknown hcl
		u := extractUnknownHcl(hclBytes, attr)
		// if we succeded in extracting the unknown hcl, add it to the list
		unknown.Merge(u)

	}
	return unknown, diags
}

func decodeSource(block *hclsyntax.Block, ctx *ConfigParseContext) (*config.Source, hcl.Diagnostics) {
	source := &config.Source{}
	source.Type = block.Labels[0]

	attrs, diags := block.Body.JustAttributes()
	if len(attrs) == 0 {
		return source, diags
	}

	unknown, diags := handleUnknownAttributes(block.AsHCLBlock(), ctx, maps.Values(attrs))
	if diags.HasErrors() {
		return source, diags
	}

	// set the unknown hcl on the source
	source.SetUnknownHcl(unknown)

	return source, diags

}

// return a shell resource for the given block
func resourceForBlock(block *hcl.Block) (modconfig.HclResource, hcl.Diagnostics) {
	// all blocks must have 2 labels - subtype and name - this is enforced by schema
	subType := block.Labels[0]
	blockName := block.Labels[1]
	fullName := config.BuildResourceName(block.Type, subType, blockName)
	factoryFuncs := map[string]func(*hcl.Block, string) (modconfig.HclResource, hcl.Diagnostics){
		config.BlockTypeCollection: config.NewCollection,
		//config.BlockTypeSource:      config.NewSource,
		//config.BlockTypeCredential:  config.NewCredential,
		//config.BlockTypeFilter:      config.NewFilter,
		//config.BlockTypeDestination: config.NewDestination,
	}

	factoryFunc, ok := factoryFuncs[block.Type]
	if !ok {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  fmt.Sprintf("resourceForBlock called for unsupported block type %s", block.Type),
			Subject:  hclhelpers.BlockRangePointer(block),
		},
		}
	}
	return factoryFunc(block, fullName)
}

func handleDecodeResult(resource modconfig.HclResource, res *parse.DecodeResult, block *hcl.Block, parseCtx *ConfigParseContext) {
	if res.Success() {
		// call post decode hook
		// NOTE: must do this BEFORE adding resource to run context to ensure we respect the base property
		moreDiags := resource.OnDecoded(block, nil)
		res.AddDiags(moreDiags)

		moreDiags = parseCtx.AddResource(resource)
		res.AddDiags(moreDiags)
		return
	}

	// failure :(
	if len(res.Depends) > 0 {
		moreDiags := parseCtx.AddDependencies(block, resource.Name(), res.Depends)
		res.AddDiags(moreDiags)
	}
}
