package cmdconfig

import (
	"github.com/spf13/viper"
	localconstants "github.com/turbot/tailpipe/internal/constants"
)

func IsLocal() bool {
	return (viper.GetString(localconstants.ConfigKeyBuiltBy) == localconstants.LocalBuild)
}
