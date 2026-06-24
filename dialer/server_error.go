package dialer

import (
	"errors"
	"strings"
)

const clientErrorResponsePrefix = "client error response. "

// ServerErrorCode identifies a structured server-side response error.
type ServerErrorCode string

const (
	ServerErrUnknown          ServerErrorCode = ""
	ServerErrNotLeader        ServerErrorCode = "NotLeader"
	ServerErrUnknownLeader    ServerErrorCode = "UnknownLeader"
	ServerErrDataNodeNotAvail ServerErrorCode = "DataNodeNotAvail"
)

// ServerError preserves the original server response while exposing structured fields.
type ServerError struct {
	Code    ServerErrorCode
	Raw     string
	Detail  string
	Address string
}

func (e *ServerError) Error() string {
	if e == nil {
		return ""
	}
	if e.Raw != "" {
		return e.Raw
	}
	return clientErrorResponsePrefix + e.Detail
}

// Is reports whether the server error matches the specified code.
func (e *ServerError) Is(code ServerErrorCode) bool {
	return e != nil && e.Code == code
}

// SuggestsFailover reports whether retrying on another node may succeed.
func (e *ServerError) SuggestsFailover() bool {
	if e == nil {
		return false
	}

	switch e.Code {
	case ServerErrNotLeader, ServerErrUnknownLeader, ServerErrDataNodeNotAvail:
		return true
	default:
		return false
	}
}

// TargetAddress returns the server-directed target node when present.
func (e *ServerError) TargetAddress() (string, bool) {
	if e == nil || e.Address == "" {
		return "", false
	}
	return e.Address, true
}

func newServerError(detail string) error {
	serverErr, _ := parseServerErrorMessage(clientErrorResponsePrefix + detail)
	return serverErr
}

// AsServerError extracts a structured server error from err. It also understands
// the legacy plain-string error format for compatibility.
func AsServerError(err error) (*ServerError, bool) {
	if err == nil {
		return nil, false
	}

	var serverErr *ServerError
	if errors.As(err, &serverErr) {
		return serverErr, true
	}

	return parseServerErrorMessage(err.Error())
}

func parseServerErrorMessage(raw string) (*ServerError, bool) {
	if raw == "" {
		return nil, false
	}

	detail := raw
	if strings.HasPrefix(raw, clientErrorResponsePrefix) {
		detail = strings.TrimPrefix(raw, clientErrorResponsePrefix)
	} else if !looksLikeStructuredServerError(raw) {
		return nil, false
	}

	serverErr := &ServerError{
		Raw:    raw,
		Detail: detail,
	}

	switch {
	case strings.Contains(detail, "<NotLeader>"):
		serverErr.Code = ServerErrNotLeader
		serverErr.Address = extractTaggedAddr(detail, "<NotLeader>")
	case strings.Contains(detail, "<UnknownLeader>"):
		serverErr.Code = ServerErrUnknownLeader
	case strings.Contains(detail, "<DataNodeNotAvail>"):
		serverErr.Code = ServerErrDataNodeNotAvail
	}

	return serverErr, true
}

func looksLikeStructuredServerError(raw string) bool {
	return strings.Contains(raw, "<NotLeader>") ||
		strings.Contains(raw, "<UnknownLeader>") ||
		strings.Contains(raw, "<DataNodeNotAvail>")
}
