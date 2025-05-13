package parse

import (
	"fmt"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
)

// reimplement this as the pipe fittings version raises an internal error

func DiagsToErrorsAndWarnings(errPrefix string, diags hcl.Diagnostics) error_helpers.ErrorAndWarnings {
	return error_helpers.NewErrorsAndWarning(
		HclDiagsToError(errPrefix, diags),
		error_helpers.HclDiagsToWarnings(diags)...,
	)
}

func HclDiagsToError(prefix string, diags hcl.Diagnostics) error {
	if !diags.HasErrors() {
		return nil
	}
	errStrings := error_helpers.DiagsToString(diags, hcl.DiagError)

	var res string
	if len(errStrings) > 0 {
		res = strings.Join(errStrings, "\n")
		if len(errStrings) > 1 {
			res += "\n"
		}
		prefixStr := ""
		if prefix != "" {
			prefixStr = prefix + ": "
		}
		// If the error string contains range information on a new line, move it to the same line
		if strings.Contains(res, "\n(") {
			parts := strings.SplitN(res, "\n(", 2)
			res = fmt.Sprintf("%s (%s", parts[0], parts[1])
		}
		return fmt.Errorf("%s%s", prefixStr, res)
	}

	return diags.Errs()[0]
}
