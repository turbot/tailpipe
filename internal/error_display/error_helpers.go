// Copied from pipe-fittings/error_helpers.go. We handle cancellation differently:
// cancellations are a user choice, so we don't throw an error (normalized to "execution cancelled").
//
//nolint:forbidigo // TODO: review fmt usage
package error_helpers

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/shiena/ansicolor"
	"github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/statushooks"
)

func init() {
	color.Output = ansicolor.NewAnsiColorWriter(os.Stderr)
}

func FailOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func FailOnErrorWithMessage(err error, message string) {
	if err != nil {
		panic(fmt.Sprintf("%s: %s", message, err.Error()))
	}
}

func ShowError(ctx context.Context, err error) {
	if err == nil {
		return
	}
	statushooks.Done(ctx)
	fmt.Fprintf(color.Error, "%s: %v\n", constants.ColoredErr, TransformErrorToSteampipe(err))
}

// ShowErrorWithMessage displays the given error nicely with the given message
func ShowErrorWithMessage(ctx context.Context, err error, message string) {
	if err == nil {
		return
	}
	statushooks.Done(ctx)
	fmt.Fprintf(color.Error, "%s: %s - %v\n", constants.ColoredErr, message, TransformErrorToSteampipe(err))
}

// TransformErrorToSteampipe removes the pq: and rpc error prefixes along
// with all the unnecessary information that comes from the
// drivers and libraries
func TransformErrorToSteampipe(err error) error {
	if err == nil {
		return nil
	}

	var errString string
	if strings.Contains(err.Error(), "flowpipe service is unreachable") {
		errString = strings.Split(err.Error(), ": ")[1]
	} else {
		errString = strings.TrimSpace(err.Error())
	}

	// an error that originated from our database/sql driver (always prefixed with "ERROR:")
	if strings.HasPrefix(errString, "ERROR:") {
		errString = strings.TrimSpace(strings.TrimPrefix(errString, "ERROR:"))
	}
	// if this is an RPC Error while talking with the plugin
	if strings.HasPrefix(errString, "rpc error") {
		// trim out "rpc error: code = Unknown desc ="
		errString = strings.TrimPrefix(errString, "rpc error: code = Unknown desc =")
	}
	return errors.New(strings.TrimSpace(errString))
}

func IsCancelledError(err error) bool {
	return errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "canceling statement due to user request")
}

func ShowWarning(warning string) {
	if len(warning) == 0 {
		return
	}
	fmt.Fprintf(color.Error, "%s: %v\n", constants.ColoredWarn, warning)
}

func PrefixError(err error, prefix string) error {
	return fmt.Errorf("%s: %s\n", prefix, TransformErrorToSteampipe(err).Error())
}
