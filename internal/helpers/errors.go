package helpers

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IsNotGRPCImplementedError checks if the provided error is a gRPC 'Unimplemented' status error.
func IsNotGRPCImplementedError(err error) bool {
	status, ok := status.FromError(err)
	if !ok {
		return false
	}

	return status.Code() == codes.Unimplemented
}
