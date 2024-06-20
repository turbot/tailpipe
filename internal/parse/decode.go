package parse

import (
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/pipe-fittings/parse"
	"github.com/turbot/tailpipe/internal/config"
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

	diags = parse.DecodeHclBody(block.Body, parseCtx.EvalCtx, parseCtx, resource)
	res.HandleDecodeDiags(diags)
	return resource, res
}

// return a shell resource for the given block
func resourceForBlock(block *hcl.Block) (modconfig.HclResource, hcl.Diagnostics) {
	// all blocks must have 2 labels - subtype and name - this is enforced by schame
	subType := block.Labels[0]
	blockName := block.Labels[1]
	fullName := config.BuildResourceName(block.Type, subType, blockName)
	factoryFuncs := map[string]func(*hcl.Block, string) (modconfig.HclResource, hcl.Diagnostics){
		config.BlockTypeCollection:  config.NewCollection,
		config.BlockTypeSource:      config.NewSource,
		config.BlockTypeCredential:  config.NewCredential,
		config.BlockTypeFilter:      config.NewFilter,
		config.BlockTypeDestination: config.NewDestination,
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
