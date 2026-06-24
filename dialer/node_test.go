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

	errType, address := pool.getNewLeader("client error response. <NotLeader>192.168.0.69:8803:dnode2")

	assert.Equal(t, NEW_LEADER, errType)
	assert.Equal(t, "192.168.0.69:8803", address)
}

func TestNodePoolParseErrorRecognizesUnknownLeader(t *testing.T) {
	pool := &nodePool{}

	errType, address := pool.parseError(newServerError("<UnknownLeader>test"))

	assert.Equal(t, UNKNOWN_LEADER, errType)
	assert.Equal(t, "", address)
}

func TestNodePoolParseErrorRecognizesDataNodeNotAvailWithoutTargetAddress(t *testing.T) {
	pool := &nodePool{}

	errType, address := pool.parseError(newServerError("<DataNodeNotAvail>test"))

	assert.Equal(t, NODE_NOT_AVAIL, errType)
	assert.Equal(t, "", address)
}

func TestNewNodePoolKeepsPrimaryAddressFirst(t *testing.T) {
	pool := newNodePool("primary:8848", []string{"secondary:8848", "primary:8848", "tertiary:8848"})

	require.Len(t, pool.nodes, 3)
	assert.Equal(t, "primary:8848", pool.nodes[0].address)
	assert.Equal(t, "secondary:8848", pool.nodes[1].address)
	assert.Equal(t, "tertiary:8848", pool.nodes[2].address)
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
