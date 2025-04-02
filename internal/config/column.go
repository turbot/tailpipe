package config

type Column struct {
	Name        string  `hcl:"name,label" cty:"name"`
	Type        *string `hcl:"type" cty:"type"`
	Source      *string `hcl:"source" cty:"source"`
	Description *string `hcl:"description" cty:"description"`
	Required    *bool   `hcl:"required" cty:"required"`
	NullValue   *string `hcl:"null_value" cty:"null_value"`
}
