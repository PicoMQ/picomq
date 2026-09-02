package picomq

import (
	"errors"
	"fmt"
)

type ErrorKind string

const (
	ErrorNotFound        ErrorKind = "not_found"
	ErrorExists          ErrorKind = "exists"
	ErrorClosed          ErrorKind = "closed"
	ErrorConflict        ErrorKind = "conflict"
	ErrorStaleEpoch      ErrorKind = "stale_epoch"
	ErrorUnauthenticated ErrorKind = "unauthenticated"
	ErrorPermission      ErrorKind = "permission_denied"
	ErrorOffsetGone      ErrorKind = "offset_gone"
	ErrorBadRequest      ErrorKind = "bad_request"
	ErrorTransport       ErrorKind = "transport"
	ErrorUnsupported     ErrorKind = "unsupported"
	ErrorInvalidResponse ErrorKind = "invalid_response"
	ErrorOther           ErrorKind = "other"
)

type ClientError struct {
	Kind    ErrorKind
	Status  int
	Code    string
	Message string
	Next    string
	Cause   error
}

func (e *ClientError) Error() string {
	message := e.Message
	if message == "" {
		message = e.Code
	}
	if e.Status != 0 {
		return fmt.Sprintf("%s (%d): %s", e.Code, e.Status, message)
	}
	return fmt.Sprintf("%s: %s", e.Code, message)
}

func (e *ClientError) Unwrap() error { return e.Cause }

func IsKind(err error, kind ErrorKind) bool {
	var target *ClientError
	return errors.As(err, &target) && target.Kind == kind
}

func unsupported(message string) error {
	return &ClientError{Kind: ErrorUnsupported, Code: "unsupported", Message: message}
}

func invalidResponse(err error) error {
	return &ClientError{Kind: ErrorInvalidResponse, Code: "invalid_response", Message: err.Error(), Cause: err}
}

func retryable(err error) bool {
	var target *ClientError
	if !errors.As(err, &target) {
		return false
	}
	return target.Kind == ErrorTransport || target.Status == 429 || target.Status >= 500 && target.Status <= 599
}
