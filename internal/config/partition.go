package config

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/v2/cty_helpers"
	"github.com/turbot/pipe-fittings/v2/hclhelpers"
	"github.com/turbot/pipe-fittings/v2/modconfig"
	"github.com/turbot/pipe-fittings/v2/plugin"
	"github.com/turbot/pipe-fittings/v2/schema"
	"github.com/turbot/pipe-fittings/v2/versionfile"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/zclconf/go-cty/cty"
)

func init() {
	registerResourceWithSubType(schema.BlockTypePartition)
}

type Partition struct {
	modconfig.HclResourceImpl
	// required to allow partial decoding
	Remain hcl.Body `hcl:",remain" json:"-"`

	// the name of the table this partition is for - this is the first label in the partition block
	TableName string

	// if the partition of for a custom table, this will be set to the custom table config
	CustomTable *Table `cty:"table"`

	// Plugin used for this partition
	Plugin *plugin.Plugin `cty:"-"`

	// Source of the data for this partition
	Source Source `cty:"source"`

	// any partition-type specific config data for the partition
	Config []byte `cty:"config"`
	// the config location
	ConfigRange hclhelpers.Range `cty:"config_range"`
	// an option filter in the format of a SQL where clause
	Filter string `cty:"filter"`
	// the sql column to use for the tp_index
	TpIndexColumn string `cty:"tp_index_column"`
}

func NewPartition(block *hcl.Block, fullName string) (modconfig.HclResource, hcl.Diagnostics) {
	if len(block.Labels) < 2 {
		return nil, hcl.Diagnostics{&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "'partition' block requires 2 labels, 'type' and 'name'",
			Subject:  hclhelpers.BlockRangePointer(block),
		}}
	}
	c := &Partition{
		HclResourceImpl: modconfig.NewHclResourceImpl(block, fullName),
		TableName:       block.Labels[0],
	}

	// NOTE: as tailpipe does not have the concept of mods, the full name is partition.<type>.<name> and
	// the unqualified name is the <type>.<name>
	c.UnqualifiedName = fmt.Sprintf("%s.%s", c.TableName, c.ShortName)
	return c, nil
}

func (p *Partition) SetConfigHcl(u *HclBytes) {
	if u == nil {
		return
	}
	p.Config = u.Hcl
	p.ConfigRange = u.Range
}

func (p *Partition) InferPluginName(v *versionfile.PluginVersionFile) string {
	// NOTE: we cannot call the TailpipeConfig.GetPluginForTable function as tailpipe config is not populated yet
	if p.CustomTable != nil {
		return constants.CorePluginInstallStream()
	}

	return GetPluginForTable(p.TableName, v.Plugins)
}

func (p *Partition) AddFilter(filter string) {
	if p.Filter == "" {
		p.Filter = filter
	} else {
		p.Filter += " and " + filter
	}
}

func (p *Partition) CollectionStatePath(collectionDir string) string {
	// return the path to the collection state file
	return filepath.Join(collectionDir, fmt.Sprintf("collection_state_%s_%s.json", p.TableName, p.ShortName))
}

func (p *Partition) Validate() hcl.Diagnostics {
	var diags hcl.Diagnostics

	// validate source block is present
	if p.Source.Type == "" {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  fmt.Sprintf("Partition '%s' is missing required source block", p.GetUnqualifiedName()),
			Subject:  p.ConfigRange.HclRange().Ptr(),
		})
	}

	// validate filter
	if p.Filter != "" {
		diags = append(diags, p.validateFilter()...)
	}

	moreDiags := p.validateIndexExpression()
	diags = append(diags, moreDiags...)
	return diags
}

// CtyValue implements CtyValueProvider
// (note this must be implemented by each resource, we cannot rely on the HclResourceImpl implementation as it will
// only serialise its own properties) )
func (p *Partition) CtyValue() (cty.Value, error) {
	return cty_helpers.GetCtyValue(p)
}

func (p *Partition) validateFilter() hcl.Diagnostics {
	var diags hcl.Diagnostics
	// check for `;` to prevent multiple statements
	if strings.Contains(p.Filter, ";") {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  fmt.Sprintf("Partition %s contains invalid filter", p.GetUnqualifiedName()),
			Detail:   "multiple expressions are not supported in partition filters, should not contain ';'.",
		})
	}
	// check for `/*`, `*/`, `--` to prevent comments
	if strings.Contains(p.Filter, "/*") || strings.Contains(p.Filter, "*/") || strings.Contains(p.Filter, "--") {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  fmt.Sprintf("Partition %s contains invalid filter", p.GetUnqualifiedName()),
			Detail:   "comments are not supported in partition filters, should not contain  comment identifiers '/*', '*/' or '--'.",
		})
	}

	forbiddenStrings := []string{
		"select ",
		"insert ",
		"update ",
		"delete ",
		"drop ",
		"create ",
		"alter ",
		"truncate ",
		"exec ",
		"execute ",
		"union ",
		"with ",
	}

	lower := strings.ToLower(p.Filter)
	for _, s := range forbiddenStrings {
		if strings.Contains(lower, s) {
			str := strings.Trim(s, " ")
			diags = append(diags, &hcl.Diagnostic{
				Severity: hcl.DiagError,
				Summary:  fmt.Sprintf("Partition %s contains invalid filter", p.GetUnqualifiedName()),
				Detail:   fmt.Sprintf("should not contain keyword '%s' in filter, unless used as a quoted identifier ('\"%s\"') to prevent unintended behavior.", str, str),
			})
		}
	}

	return diags
}

func (p *Partition) validateIndexExpression() hcl.Diagnostics {
	var diags hcl.Diagnostics

	if p.TpIndexColumn == "" {
		p.TpIndexColumn = "'default'"
		return diags
	}

	// check for `;` to prevent multiple statements
	if strings.Contains(p.Filter, ";") {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  fmt.Sprintf("Partition %s contains invalid filter", p.GetUnqualifiedName()),
			Detail:   "multiple expressions are not supported in partition filters, should not contain ';'.",
			Subject:  p.GetDeclRange(),
		})
	}
	// check for `/*`, `*/`, `--` to prevent comments
	if strings.Contains(p.Filter, "/*") || strings.Contains(p.Filter, "*/") || strings.Contains(p.Filter, "--") {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  fmt.Sprintf("Partition %s contains invalid filter", p.GetUnqualifiedName()),
			Detail:   "comments are not supported in partition filters, should not contain  comment identifiers '/*', '*/' or '--'.",
			Subject:  p.GetDeclRange(),
		})
	}
	if diags.HasErrors() {
		return diags
	}

	// tp_index must be a column name - validate it
	if !IsColumnName(p.TpIndexColumn) {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  fmt.Sprintf("Partition %s has an invalid tp_index expression", p.GetUnqualifiedName()),
			Detail:   fmt.Sprintf("tp_index '%s' is not a valid column name. It should be a simple column name without any SQL expressions or functions.", p.TpIndexColumn),
			Subject:  p.GetDeclRange(),
		})
		return diags
	}
	// wrap in double quotes
	p.TpIndexColumn = fmt.Sprintf(`"%s"`, p.TpIndexColumn)

	return diags
}

// GetFormat returns the format for this partition, if either the source or the custom table has one
func (p *Partition) GetFormat() *Format {
	var format = p.Source.Format
	if format == nil && p.CustomTable != nil {
		// if the source does not provide a format, use the custom table format
		format = p.CustomTable.DefaultSourceFormat
	}
	return format
}

func (p *Partition) FormatSupportsDirectConversion() bool {
	format := p.GetFormat()
	if format == nil {
		return false
	}
	return table.FormatSupportsDirectConversion(format.Type)
}

// EqualConfig compares 2 partitions for configuration equality, excluding Source.Config.Hcl bytes
func (p *Partition) EqualConfig(other *Partition) bool {
	if p == nil || other == nil {
		return p == other
	}
	// Hcl resource identity/stable metadata (ignore DeclRange for stability)
	if p.HclResourceImpl.FullName != other.HclResourceImpl.FullName ||
		p.HclResourceImpl.ShortName != other.HclResourceImpl.ShortName ||
		p.HclResourceImpl.UnqualifiedName != other.HclResourceImpl.UnqualifiedName ||
		p.HclResourceImpl.BlockType != other.HclResourceImpl.BlockType {
		return false
	}
	if p.TableName != other.TableName {
		return false
	}
	// Source (exclude Config.Hcl as its ordering is not stable)
	if p.Source.Type != other.Source.Type {
		return false
	}
	if (p.Source.Connection == nil) != (other.Source.Connection == nil) {
		return false
	}
	if p.Source.Connection != nil && other.Source.Connection != nil {
		// compare referenced connection identity by unqualified name only
		if p.Source.Connection.HclResourceImpl.UnqualifiedName != other.Source.Connection.HclResourceImpl.UnqualifiedName {
			return false
		}
	}
	// Compare Source.Format by stable identity only when both present (ignore presence differences)
	if p.Source.Format != nil && other.Source.Format != nil {
		pf := p.Source.Format
		of := other.Source.Format
		if pf.Type != of.Type {
			return false
		}
		if pf.PresetName != of.PresetName {
			return false
		}
		if pf.HclResourceImpl.FullName != of.HclResourceImpl.FullName ||
			pf.HclResourceImpl.ShortName != of.HclResourceImpl.ShortName ||
			pf.HclResourceImpl.UnqualifiedName != of.HclResourceImpl.UnqualifiedName ||
			pf.HclResourceImpl.BlockType != of.HclResourceImpl.BlockType {
			return false
		}
	}
	if (p.Source.Config == nil) != (other.Source.Config == nil) {
		return false
	}
	if p.Source.Config != nil && !reflect.DeepEqual(p.Source.Config.Range, other.Source.Config.Range) {
		return false
	}
	// Partition-level config: treat nil and empty as equal; compare range only when there is content
	if !(len(p.Config) == 0 && len(other.Config) == 0) {
		if !reflect.DeepEqual(p.Config, other.Config) {
			return false
		}
		if !reflect.DeepEqual(p.ConfigRange, other.ConfigRange) {
			return false
		}
	}
	if p.Filter != other.Filter || p.TpIndexColumn != other.TpIndexColumn {
		return false
	}
	// Custom table presence
	if (p.CustomTable == nil) != (other.CustomTable == nil) {
		return false
	}
	if p.CustomTable != nil && other.CustomTable != nil {
		if !reflect.DeepEqual(p.CustomTable, other.CustomTable) {
			return false
		}
	}
	// Plugin: compare only stable identity when both present (ignore presence differences)
	if p.Plugin != nil && other.Plugin != nil {
		if p.Plugin.Instance != other.Plugin.Instance ||
			p.Plugin.Alias != other.Plugin.Alias ||
			p.Plugin.Plugin != other.Plugin.Plugin {
			return false
		}
	}
	return true
}
