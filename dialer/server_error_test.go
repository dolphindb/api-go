package dialer

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewServerErrorPreservesOriginalMessage(t *testing.T) {
	err := newServerError("<UnknownLeader>leader is temporarily unavailable")

	serverErr, ok := AsServerError(err)
	require.True(t, ok)
	assert.Equal(t, "client error response. <UnknownLeader>leader is temporarily unavailable", serverErr.Error())
	assert.Equal(t, ServerErrUnknownLeader, serverErr.Code)
	assert.Equal(t, "<UnknownLeader>leader is temporarily unavailable", serverErr.Detail)
	assert.True(t, serverErr.SuggestsFailover())
}

func TestAsServerErrorNormalizesLegacyStringError(t *testing.T) {
	serverErr, ok := AsServerError(errors.New("client error response. <NotLeader>192.168.0.69:8803:dnode2"))

	require.True(t, ok)
	assert.Equal(t, ServerErrNotLeader, serverErr.Code)
	assert.Equal(t, "192.168.0.69:8803", serverErr.Address)
	addr, hasTarget := serverErr.TargetAddress()
	assert.True(t, hasTarget)
	assert.Equal(t, "192.168.0.69:8803", addr)
	assert.True(t, serverErr.SuggestsFailover())
}

func TestAsServerErrorDoesNotExtractDataNodeNotAvailAddress(t *testing.T) {
	serverErr, ok := AsServerError(errors.New("client error response. <DataNodeNotAvail>node is unavailable"))

	require.True(t, ok)
	assert.Equal(t, ServerErrDataNodeNotAvail, serverErr.Code)
	assert.Equal(t, "", serverErr.Address)
	_, hasTarget := serverErr.TargetAddress()
	assert.False(t, hasTarget)
	assert.True(t, serverErr.SuggestsFailover())
}

func TestAsServerErrorRejectsNonServerErrors(t *testing.T) {
	serverErr, ok := AsServerError(errors.New("dial tcp 127.0.0.1:8848: connect: connection refused"))

	assert.False(t, ok)
	assert.Nil(t, serverErr)
}
