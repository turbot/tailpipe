package parse

import (
	"fmt"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/turbot/pipe-fittings/hclhelpers"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/pipe-fittings/parse"
	"github.com/turbot/pipe-fittings/schema"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/zclconf/go-cty/cty/gocty"
	"golang.org/x/exp/maps"
)

func decodeTailpipeConfig(parseCtx *ConfigParseContext) (*config.TailpipeConfig, hcl.Diagnostics) {
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

	var tailpipeConfig = config.NewTailpipeConfig()
	for _, block := range blocksToDecode {
		resource, res := decodeBlock(block, parseCtx)
		diags = append(diags, res.Diags...)
		if !res.Success() || resource == nil {
			continue
		}
		err := tailpipeConfig.Add(resource)
		if err != nil {
			diags = append(diags, &hcl.Diagnostic{
				Severity: hcl.DiagError,
				Summary:  fmt.Sprintf("failed to add resource %s to config", resource.Name()),
				Detail:   err.Error(),
				Subject:  hclhelpers.BlockRangePointer(block),
			})
		}

	}
	return tailpipeConfig, diags
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

	// handle the result
	// - if there are dependencies, add to run context
	handleDecodeResult(resource, res, block, parseCtx)

	return resource, res
}

// generic decode function for any resource we do not have custom decodeTailpipeConfig logic for
func decodeResource(block *hcl.Block, parseCtx *ConfigParseContext) (modconfig.HclResource, *parse.DecodeResult) {
	res := parse.NewDecodeResult()
	// get shell resource
	resource, diags := resourceForBlock(block)
	if diags != nil && diags.HasErrors() {
		res.HandleDecodeDiags(diags)
		return nil, res
	}

	switch block.Type {
	case schema.BlockTypePartition:
		return decodePartition(block, parseCtx, resource)
	case schema.BlockTypeConnection:
		return decodeConnection(block, parseCtx, resource)
	default:
		// TODO what resources does this include?
		diags = parse.DecodeHclBody(block.Body, parseCtx.EvalCtx, parseCtx, resource)
		res.HandleDecodeDiags(diags)
		return resource, res
	}
}

func decodePartition(block *hcl.Block, parseCtx *ConfigParseContext, resource modconfig.HclResource) (modconfig.HclResource, *parse.DecodeResult) {
	res := parse.NewDecodeResult()

	target := resource.(*config.Partition)
	syntaxBody := block.Body.(*hclsyntax.Body)

	attrs := syntaxBody.Attributes
	blocks := syntaxBody.Blocks

	var unknownAttrs []*hcl.Attribute
	for _, attr := range attrs {
		unknownAttrs = append(unknownAttrs, attr.AsHCLAttribute())
	}

	var unknownBlocks []*hcl.Block
	for _, block := range blocks {
		// TODO K decode plugin block
		switch block.Type {
		case "source":
			// decode source block
			source, sourceRes := decodeSource(block, parseCtx)
			res.Merge(sourceRes)

			if !res.Diags.HasErrors() {
				target.Source = *source
			}
		default:
			unknownBlocks = append(unknownBlocks, block.AsHCLBlock())
		}
	}

	// convert the unknown blocks and attributes to the raw hcl bytes
	unknown, diags := handleUnknownHcl(block, parseCtx, unknownAttrs, unknownBlocks)
	res.HandleDecodeDiags(diags)
	if !res.Diags.HasErrors() {
		// now set unknown hcl
		target.SetConfigHcl(unknown)
	}

	return target, res
}

func decodeConnection(block *hcl.Block, parseCtx *ConfigParseContext, resource modconfig.HclResource) (modconfig.HclResource, *parse.DecodeResult) {
	res := parse.NewDecodeResult()

	target := resource.(*config.TailpipeConnection)
	// we just want the hck for the attributes - not the whole block (i.e. we want to exclude the block name/type/brackets)
	syntaxBody, ok := block.Body.(*hclsyntax.Body)
	if !ok {
		// unexpected
		res.AddDiags(hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "failed to decode connection block",
			Detail:   fmt.Sprintf("unexpected block body type %T - expected *hclsyntax.Body", block.Body),
			Subject:  hclhelpers.BlockRangePointer(block),
		}})
		return nil, res
	}
	hclBytes := &config.HclBytes{}
	for _, attr := range syntaxBody.Attributes {
		hclBytes.Merge(config.HclBytesForRange(parseCtx.FileData[block.DefRange.Filename], attr.Range()))
	}
	target.Hcl = hclBytes.Hcl
	target.HclRange = hclhelpers.NewRange(hclBytes.Range)
	return target, res
}

func handleUnknownHcl(block *hcl.Block, parseCtx *ConfigParseContext, unknownAttrs []*hcl.Attribute, unknownBlocks []*hcl.Block) (*config.HclBytes, hcl.Diagnostics) {
	var diags hcl.Diagnostics
	unknown := &config.HclBytes{}
	for _, attr := range unknownAttrs {
		//	get the hcl bytes for the file
		hclBytes := parseCtx.FileData[block.DefRange.Filename]
		// extract the unknown hcl
		u := config.HclBytesForRange(hclBytes, attr.Range)
		// if we succeded in extracting the unknown hcl, add it to the list
		unknown.Merge(u)
	}
	for _, block := range unknownBlocks {
		//	get the hcl bytes for the file
		hclBytes := parseCtx.FileData[block.DefRange.Filename]
		// extract the unknown hcl
		u := config.HclBytesForRange(hclBytes, hclhelpers.BlockRangeWithLabels(block))
		// if we succeded in extracting the unknown hcl, add it to the list
		unknown.Merge(u)
	}
	return unknown, diags
}

func decodeSource(block *hclsyntax.Block, parseCtx *ConfigParseContext) (*config.Source, *parse.DecodeResult) {
	res := parse.NewDecodeResult()
	source := &config.Source{}
	source.Type = block.Labels[0]

	attrs, diags := block.Body.JustAttributes()
	res.HandleDecodeDiags(diags)
	if len(attrs) == 0 {
		return source, res
	}
	var blocks []*hcl.Block
	for _, block := range block.Body.Blocks {
		blocks = append(blocks, block.AsHCLBlock())
	}

	// special case for connection
	if connectionAttr, ok := attrs["connection"]; ok {
		//try to evaluate expression
		val, diags := connectionAttr.Expr.Value(parseCtx.EvalCtx)
		res.HandleDecodeDiags(diags)
		// we failed, possibly as result of dependency error - give up for now
		if !res.Success() {
			return source, res
		}
		var conn = &config.TailpipeConnection{}
		err := gocty.FromCtyValue(val, conn)
		if err != nil {
			// failed to decode connection
			res.AddDiags(hcl.Diagnostics{&hcl.Diagnostic{
				Severity: hcl.DiagError,
				Summary:  "failed to decode connection",
				Detail:   fmt.Sprintf("failed to decode connection: %s", err.Error()),
				Subject:  hclhelpers.BlockRangePointer(block.AsHCLBlock()),
			}})
			return source, res
		}
		source.Connection = conn
	}
	delete(attrs, "connection")

	// get the unknown hcl
	unknown, diags := handleUnknownHcl(block.AsHCLBlock(), parseCtx, maps.Values(attrs), blocks)
	res.HandleDecodeDiags(diags)
	if res.Diags.HasErrors() {
		return source, res
	}

	// set the unknown hcl on the source
	source.SetConfigHcl(unknown)

	return source, res

}

// return a shell resource for the given block
func resourceForBlock(block *hcl.Block) (modconfig.HclResource, hcl.Diagnostics) {
	// all blocks must have 2 labels - subtype and name - this is enforced by schema
	subType := block.Labels[0]
	blockName := block.Labels[1]
	fullName := config.BuildResourceName(block.Type, subType, blockName)
	factoryFuncs := map[string]func(*hcl.Block, string) (modconfig.HclResource, hcl.Diagnostics){
		schema.BlockTypePartition:  config.NewPartition,
		schema.BlockTypeConnection: config.NewTailpipeConnection,
		//schema.BlockTypePlugin: config.NewPlugin,
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
	if res.Success() && resource != nil {
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
