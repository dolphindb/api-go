package errors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrors(t *testing.T) {
	err := InvalidResponseError("invalid")
	assert.Equal(t, err.Error(), "invalid response format. invalid")

	err = InvalidByteOrderError(2)
	assert.Equal(t, err.Error(), "invalid byte order 2")

	err = ResponseNotOKError([]byte("internal error"))
	assert.Equal(t, err.Error(), "client error response. internal error")

	err = ReadDataTypeAndDataFormError("%$")
	assert.Equal(t, err.Error(), "failed to read DataType and DataForm. %$")
}
