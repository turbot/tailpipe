package collector

import "fmt"

type ExecutionError struct {
	executionId string
	err         error
}

func NewExecutionError(err error, executionId string) error {
	if err == nil {
		return nil
	}
	return &ExecutionError{
		executionId: executionId,
		err:         err,
	}
}

// Unwrap implements error wrapping.
func (e *ExecutionError) Unwrap() error {
	return e.err
}

func (e ExecutionError) Error() string {
	return fmt.Sprintf("execution %s failed: %s", e.executionId, e.err)
}
