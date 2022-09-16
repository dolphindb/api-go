package errors

import (
	"fmt"
)

var (
// ErrHeaderWrongPart         = errors.New("HEADER not 3 PARTS")
// ErrHeaderInvalidEndianness = errors.New("HEADER CONTAINS INVALID ENDIANNESS BYTE")
// ErrParseVectorFailed       = errors.New("PARSE VECTOR FAILED")
// ErrParseScalarFailed       = errors.New("PARSE SCALAR FAILED")
)

// InvalidResponseError ...
func InvalidResponseError(msg string) error {
	return fmt.Errorf("invalid response format. %s", msg)
}

// InvalidByteOrderError ...
func InvalidByteOrderError(b byte) error {
	return fmt.Errorf("invalid byte order %v", b)
}

// ResponseNotOKError ...
func ResponseNotOKError(resp []byte) error {
	return fmt.Errorf("client error response. %v", string(resp))
}

// ReadDataTypeAndDataFormError ...
func ReadDataTypeAndDataFormError(msg string) error {
	return fmt.Errorf("failed to read DataType and DataForm. %s", msg)
}
