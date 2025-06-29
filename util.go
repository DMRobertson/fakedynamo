package fakedynamo

import (
	"fmt"
	"net/http"
)

func ptr[T any](v T) *T {
	return &v
}

func val[T any](p *T) T {
	return *p
}

type dummyAwsError struct {
	// awserror.Error
	code    string
	message string
	// awserror.RequestFailure
	httpStatusCode int
}

func (d dummyAwsError) Error() string {
	return fmt.Sprintf("%s: %s", d.code, d.message)
}

func (d dummyAwsError) Code() string {
	return d.code
}

func (d dummyAwsError) Message() string {
	return d.message
}

func (d dummyAwsError) OrigErr() error {
	return nil
}

func (d dummyAwsError) RequestID() string {
	return ""
}

// The DynamoDB service doesn't explicitly define a ValidationException error, so it doesn't show up in the
// https://github.com/aws/aws-sdk/issues/47 and https://github.com/aws/aws-sdk-go-v2/issues/3040
func newValidationException(message string) error {
	return dummyAwsError{
		code:           "ValidationException",
		message:        message,
		httpStatusCode: http.StatusBadRequest,
	}
}
