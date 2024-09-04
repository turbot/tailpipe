package plugin

import (
	"fmt"

	"github.com/turbot/pipe-fittings/ociinstaller"
	"github.com/turbot/pipe-fittings/utils"
)

type PluginRemoveReport struct {
	Image     *ociinstaller.ImageRef
	ShortName string
}

type PluginRemoveReports []PluginRemoveReport

func (r PluginRemoveReports) Print() {
	length := len(r)
	if length > 0 {
		fmt.Printf("\nUninstalled %s:\n", utils.Pluralize("plugin", length)) //nolint:forbidigo // acceptable
		for _, report := range r {
			org, name, _ := report.Image.GetOrgNameAndStream()
			fmt.Printf("* %s/%s\n", org, name) //nolint:forbidigo // acceptable

		}
	}
}
