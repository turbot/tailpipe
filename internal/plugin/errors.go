package plugin

import (
	"errors"
	"strings"
)

// cleanupPluginError is a utility function to clean up errors returned the plugin event stream
// and make them more user-friendly. It handles specific error cases, such as RPC errors and in particular
// EOF errors caused by plugin crashes, and transforms them into more meaningful messages.
func cleanupPluginError(err error) error {
	if err == nil {
		return nil
	}

	errString := strings.TrimSpace(err.Error())

	// if this is an RPC Error while talking with the plugin
	if strings.HasPrefix(errString, "rpc error") {
		if strings.Contains(errString, "error reading from server: EOF") {
			errString = "lost connection to plugin"
		} else {
			// trim out "rpc error: code = Unknown desc ="
			errString = strings.TrimPrefix(errString, "rpc error: code = Unknown desc =")
		}
	}
	return errors.New(strings.TrimSpace(errString))
}
