package parse

import (
	"fmt"
	"github.com/zclconf/go-cty/cty/gocty"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/turbot/pipe-fittings/v2/hclhelpers"
	"github.com/turbot/pipe-fittings/v2/modconfig"
	"github.com/turbot/pipe-fittings/v2/parse"
	"github.com/turbot/pipe-fittings/v2/schema"
	"github.com/turbot/tailpipe/internal/config"
)

func decodeTailpipeConfig(parseCtx *ConfigParseContext) hcl.Diagnostics {
	var diags hcl.Diagnostics
	blocksToDecode, err := parseCtx.BlocksToDecode()
	// build list of blocks to decode
	if err != nil {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "failed to determine required dependency order",
			Detail:   err.Error()})
		return diags
	}

	// now clear dependencies from run context - they will be rebuilt
	parseCtx.ClearDependencies()

	for _, block := range blocksToDecode {
		resource, res := decodeBlock(block, parseCtx)
		diags = append(diags, res.Diags...)
		if !res.Success() || resource == nil {
			continue
		}
		err := parseCtx.tailpipeConfig.Add(resource)
		if err != nil {
			diags = append(diags, &hcl.Diagnostic{
				Severity: hcl.DiagError,
				Summary:  fmt.Sprintf("failed to add resource %s to config", resource.Name()),
				Detail:   err.Error(),
				Subject:  hclhelpers.BlockRangePointer(block),
			})
		}

	}
	return diags
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
		res = decodePartition(block, parseCtx, resource)
	case schema.BlockTypeConnection:
		res = decodeConnection(block, parseCtx, resource)
	case schema.BlockTypeFormat:
		res = decodeFormat(block, parseCtx, resource)
	// TODO #parsing to support inline Format we need to manually parse the table block https://github.com/turbot/tailpipe/issues/109
	case schema.BlockTypeTable:
		diags = parse.DecodeHclBody(block.Body, parseCtx.EvalCtx, parseCtx, resource)
		tableRes := parse.NewDecodeResult()
		tableRes.HandleDecodeDiags(diags)
		// if the res has a dependency because of a format preset, resolve it
		if len(tableRes.Depends) > 0 {
			var formatPreset string
			if formatPreset, tableRes = extractPresetNameFromDependencyError(parseCtx, tableRes); formatPreset != "" {
				format, diags := config.NewPresetFormat(block, formatPreset)
				if diags != nil && diags.HasErrors() {
					res.AddDiags(diags)
				} else {
					// add the format preset to the table
					resource.(*config.Table).DefaultSourceFormat = format
				}
			}
		}
		// merge the table decode result with the main decode result
		res.Merge(tableRes)

	default:
		diags = parse.DecodeHclBody(block.Body, parseCtx.EvalCtx, parseCtx, resource)
		res.HandleDecodeDiags(diags)
	}

	return resource, res
}

func decodePartition(block *hcl.Block, parseCtx *ConfigParseContext, resource modconfig.HclResource) *parse.DecodeResult {
	res := parse.NewDecodeResult()

	target := resource.(*config.Partition)
	syntaxBody := block.Body.(*hclsyntax.Body)

	attrs := syntaxBody.Attributes
	blocks := syntaxBody.Blocks

	var unknownAttrs []*hcl.Attribute
	for _, attr := range attrs {
		switch attr.Name {
		case "filter":
			//try to evaluate expression
			val, diags := attr.Expr.Value(parseCtx.EvalCtx)
			res.HandleDecodeDiags(diags)
			// we failed, possibly as result of dependency error - give up for now
			if !res.Success() {
				return res
			}
			target.Filter = val.AsString()
		default:
			unknownAttrs = append(unknownAttrs, attr.AsHCLAttribute())
		}
	}
	var unknownBlocks []*hcl.Block
	for _, block := range blocks {
		switch block.Type {
		case schema.BlockTypeSource:
			// decode source block
			source, sourceRes := decodeSource(block, parseCtx)
			res.Merge(sourceRes)
			if res.Success() {
				target.Source = *source
			}
		default:
			unknownBlocks = append(unknownBlocks, block.AsHCLBlock())
		}
	}

	if !res.Success() {
		return res
	}

	// convert the unknown blocks and attributes to the raw hcl bytes
	unknown, diags := handleUnknownHcl(block, parseCtx, unknownAttrs, unknownBlocks)
	res.HandleDecodeDiags(diags)
	if res.Success() {
		// now set unknown hcl
		target.SetConfigHcl(unknown)
	}

	return res
}

// extractPresetNameFromDependencyError checks if the given decode result has a depedency which is because of a format preset and
// if so, retrieve the preset name from the dependency error
func extractPresetNameFromDependencyError(parseCtx *ConfigParseContext, res *parse.DecodeResult) (string, *parse.DecodeResult) {
	for depName := range res.Depends {
		if _, ok := config.GetPluginForFormatPreset(depName, parseCtx.pluginVersionFile.Plugins); ok {
			delete(res.Depends, depName)
			return depName, res
		}
	}

	return "", res
}

func decodeConnection(block *hcl.Block, parseCtx *ConfigParseContext, resource modconfig.HclResource) *parse.DecodeResult {
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
		return res
	}
	hclBytes := &config.HclBytes{}
	for _, attr := range syntaxBody.Attributes {
		hclBytes.Merge(config.HclBytesForRange(parseCtx.FileData[block.DefRange.Filename], attr.Range()))
	}
	target.Hcl = hclBytes.Hcl
	target.HclRange = hclBytes.Range
	return res
}

func handleUnknownHcl(block *hcl.Block, parseCtx *ConfigParseContext, unknownAttrs []*hcl.Attribute, unknownBlocks []*hcl.Block) (*config.HclBytes, hcl.Diagnostics) {
	var diags hcl.Diagnostics
	unknown := &config.HclBytes{}
	for _, attr := range unknownAttrs {
		//	get the hcl bytes for the file
		hclBytes := parseCtx.FileData[block.DefRange.Filename]
		// extract the unknown hcl - taking full lines
		// (we do this in case the attribute value was a grok() function call, meaning it has been escaped,
		// so the attribute byte range will not match the raw hcl filedata)
		u := config.HclBytesForLines(hclBytes, attr.Range)
		// if we succeeded in extracting the unknown hcl, add it to the list
		unknown.Merge(u)
	}
	for _, block := range unknownBlocks {
		//	get the hcl bytes for the blocks - taking full lines
		// (we do this in case the attribute value was a grok() function call, meaning it has been escaped,
		// so the attribute byte range will not match the raw hcl filedata)
		hclBytes := parseCtx.FileData[block.DefRange.Filename]
		// extract the unknown hcl
		u := config.HclBytesForLines(hclBytes, hclhelpers.BlockRangeWithLabels(block))
		// if we succeded in extracting the unknown hcl, add it to the list
		unknown.Merge(u)
	}
	return unknown, diags
}

func decodeSource(block *hclsyntax.Block, parseCtx *ConfigParseContext) (*config.Source, *parse.DecodeResult) {
	res := parse.NewDecodeResult()
	source := &config.Source{}
	source.Type = block.Labels[0]

	var unknownBlocks []*hcl.Block
	for _, block := range block.Body.Blocks {
		unknownBlocks = append(unknownBlocks, block.AsHCLBlock())
	}
	attrMap, diags := block.Body.JustAttributes()
	res.HandleDecodeDiags(diags)
	if !res.Success() {
		return nil, res
	}

	if len(attrMap)+len(unknownBlocks) == 0 {
		return source, res
	}

	var unknownAttrs []*hcl.Attribute
	for attrName, attr := range attrMap {
		switch attrName {

		case schema.AttributeConnection:
			// resolve the connection reference
			conn, connRes := resolveReference[*config.TailpipeConnection](parseCtx, attr)
			res.Merge(connRes)
			if res.Success() {
				source.Connection = conn
			}
		case schema.AttributeFormat:
			// resolve the format reference
			format, formatRes := resolveReference[*config.Format](parseCtx, attr)
			if len(formatRes.Depends) > 0 {
				// if the res has a dependency because of a format preset, resolve it
				formatPreset, formatRes := extractPresetNameFromDependencyError(parseCtx, formatRes)
				if formatPreset != "" {
					format, diags = config.NewPresetFormat(block.AsHCLBlock(), formatPreset)
					if diags != nil && diags.HasErrors() {
						formatRes.AddDiags(diags)
					}
					// fall through to add the format to the source
				}
			}

			res.Merge(formatRes)
			if res.Success() {
				source.Format = format
			}

		default:
			unknownAttrs = append(unknownAttrs, attr)
		}

	}

	// if we failed for any reason (including dependency errors) give up fdor now
	if !res.Success() {
		return source, res
	}

	// get the unknown hcl
	unknown, diags := handleUnknownHcl(block.AsHCLBlock(), parseCtx, unknownAttrs, unknownBlocks)
	res.HandleDecodeDiags(diags)
	if !res.Success() {
		return source, res
	}

	// set the unknown hcl on the source
	source.SetConfigHcl(unknown)

	return source, res

}

func resourceFromAttr(parseCtx *ConfigParseContext, attr *hcl.Attribute, target any) *parse.DecodeResult {
	var res = parse.NewDecodeResult()
	//try to evaluate expression
	val, diags := attr.Expr.Value(parseCtx.EvalCtx)

	res.HandleDecodeDiags(diags)
	// we failed, possibly as result of dependency error - give up for now
	if !res.Success() {
		return res
	}

	err := gocty.FromCtyValue(val, target)
	if err != nil {
		// failed to decode connection
		res.AddDiags(hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "failed to decode expression",
			Detail:   fmt.Sprintf("failed to decode expression: %s", err.Error()),
			Subject:  attr.Range.Ptr(),
		}})
	}
	return res
}

// resolveReference resolves a reference to a resource in the parse context
// this is similar to the pipe-fittings function parse.resolveReferences
func resolveReference[T any](parseCtx *ConfigParseContext, attr *hcl.Attribute) (T, *parse.DecodeResult) {
	var res = parse.NewDecodeResult()
	var empty T
	// if the expression is a reference, use the resource provider to resolve it

	// convert the expression to a scope traversal
	scopeTraversal, ok := attr.Expr.(*hclsyntax.ScopeTraversalExpr)
	if !ok {
		res.AddDiags(hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  fmt.Sprintf("attribute does not contain a reference: %s", attr.Name),
			Detail:   fmt.Sprintf("resolveReference does not support expression type %T", attr.Expr),
			Subject:  attr.Range.Ptr(),
		}})
		// otherwise fail
		return empty, res
	}

	// convert the scope traversal to a property path string
	path := hclhelpers.TraversalAsString(scopeTraversal.Traversal)
	// parse this as a resource name
	parsedName, err := modconfig.ParseResourceName(path)
	if err != nil {
		res.AddDiags(hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  fmt.Sprintf("failed to decode attribute %s", attr.Name),
			Detail:   fmt.Sprintf("failed to parse resource referecnes %s", path),
			Subject:  attr.Range.Ptr(),
		}})

		return empty, res
	}

	// does the parse context contain this resource?
	if r, ok := parseCtx.GetResource(parsedName); ok {
		// convert the resource to the target type
		typedRes, ok := r.(T)
		if !ok {
			res.AddDiags(hcl.Diagnostics{&hcl.Diagnostic{
				Severity: hcl.DiagError,
				Summary:  fmt.Sprintf("failed to decode attribute %s", attr.Name),
				Detail:   fmt.Sprintf("resource %s is not of type %T", parsedName.ToResourceName(), typedRes),
				Subject:  attr.Range.Ptr(),
			}})
			return empty, res
		}

		// success!
		return typedRes, res
	}

	// otherwise add the dependency to the resource
	res.Depends[parsedName.ToResourceName()] = &modconfig.ResourceDependency{Range: attr.Expr.Range(), Traversals: attr.Expr.Variables()}

	return empty, res

}

func decodeFormat(block *hcl.Block, parseCtx *ConfigParseContext, resource modconfig.HclResource) *parse.DecodeResult {
	res := parse.NewDecodeResult()
	format := resource.(*config.Format)

	// decode the body using DecodeHclBody, to ensure the HclResource base structs get parsed
	// NOTE: ignore any diagnostics - there will be unknown attributes
	// just ensure the type is set
	diags := parse.DecodeHclBodyIntoStruct(block.Body, parseCtx.EvalCtx, parseCtx, resource.GetHclResourceImpl())
	res.HandleDecodeDiags(diags)

	attrMap, diags := block.Body.JustAttributes()
	res.HandleDecodeDiags(diags)
	if !res.Success() {
		return res
	}
	var unknownAttrs []*hcl.Attribute
	for attrName, attr := range attrMap {
		switch attrName {
		case schema.AttributeType:
			var ty string
			res.Merge(resourceFromAttr(parseCtx, attr, &ty))
			if res.Success() {
				format.Type = ty
			}
		default:
			unknownAttrs = append(unknownAttrs, attr)
		}
	}

	// blocks are not allowed so no need to check for unknown blocks

	// get the unknown hcl
	unknown, diags := handleUnknownHcl(block, parseCtx, unknownAttrs, nil)
	res.HandleDecodeDiags(diags)
	if !res.Success() {
		return res
	}

	// set the unknown hcl on the source
	format.SetConfigHcl(unknown)

	return res
}

// return a shell resource for the given block
func resourceForBlock(block *hcl.Block) (modconfig.HclResource, hcl.Diagnostics) {
	factoryFuncs := map[string]func(*hcl.Block, string) (modconfig.HclResource, hcl.Diagnostics){
		schema.BlockTypePartition:  config.NewPartition,
		schema.BlockTypeConnection: config.NewTailpipeConnection,
		schema.BlockTypeTable:      config.NewTable,
		schema.BlockTypeFormat:     config.NewFormat,
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

	name := fmt.Sprintf("%s.%s", block.Type, strings.Join(block.Labels, "."))

	parsedName, err := modconfig.ParseResourceName(name)
	if err != nil {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  fmt.Sprintf("failed to parse resource name %s", name),
			Detail:   err.Error(),
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}
	return factoryFunc(block, parsedName.ToResourceName())
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
	if len(res.Depends) > 0 && resource != nil {
		moreDiags := parseCtx.AddDependencies(block, resource.Name(), res.Depends)
		res.AddDiags(moreDiags)
	}
}
