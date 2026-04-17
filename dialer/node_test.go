package dialer

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodePoolGetNewLeaderParsesAddressAfterMarker(t *testing.T) {
	pool := &nodePool{}
	target := &node{}

	errType := pool.getNewLeader("client error response. <NotLeader>192.168.0.69:8803:dnode2", target)

	assert.Equal(t, NEW_LEADER, errType)
	assert.Equal(t, "192.168.0.69:8803", target.address)
}

func TestNodePoolHandleNotAvailErrorParsesAddressAfterMarker(t *testing.T) {
	pool := &nodePool{}
	target := &node{}

	errType := pool.handleNotAvailError("client error response. <DataNodeNotAvail>192.168.0.69:8803:dnode2", target)

	assert.Equal(t, NODE_NOT_AVAIL, errType)
	assert.Equal(t, "192.168.0.69:8803", target.address)
}

func TestNodePoolParseErrorRecognizesUnknownLeader(t *testing.T) {
	pool := &nodePool{}
	target := &node{}

	errType := pool.parseError(newServerError("<UnknownLeader>test"), target)

	assert.Equal(t, UNKNOWN_LEADER, errType)
	assert.Equal(t, "", target.address)
}

func TestConnectNodeTreatsUnknownLeaderAsFailoverSignal(t *testing.T) {
	originalConnect := connectToAddress
	defer func() {
		connectToAddress = originalConnect
	}()

	connectToAddress = func(c *conn, addr string) error {
		return errors.New("client error response. <UnknownLeader>test")
	}

	rawConn, err := NewConn(context.TODO(), "primary:8848", &BehaviorOptions{
		EnableHighAvailability: true,
		HighAvailabilitySites:  []string{"secondary:8848"},
	})
	require.NoError(t, err)

	internalConn := rawConn.(*conn)
	internalConn.nodePool = newNodePool("primary:8848", []string{"secondary:8848"})

	connected, err := internalConn.connectNode(&node{address: "primary:8848"})
	assert.False(t, connected)
	assert.NoError(t, err)
}
