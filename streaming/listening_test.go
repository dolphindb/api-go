package streaming

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

type testAbstractClient struct {
	sub  *subscriber
	exit chan bool
}

func (t *testAbstractClient) activeCloseConnection(req *SubscribeRequest) error { return nil }
func (t *testAbstractClient) doReconnect(req *SubscribeRequest) bool            { return false }
func (t *testAbstractClient) getSubscriber() *subscriber                        { return t.sub }
func (t *testAbstractClient) getConn() (net.Conn, bool)                         { return nil, false }
func (t *testAbstractClient) subscribe(req *SubscribeRequest) error             { return nil }
func (t *testAbstractClient) UnSubscribe(req *SubscribeRequest) error {
	return nil
}
func (t *testAbstractClient) IsClosed() bool {
	select {
	case <-t.exit:
		return true
	default:
		return false
	}
}

func TestStartListeningReturnsErrorWhenPortUnavailable(t *testing.T) {
	client := &testAbstractClient{
		sub:  newSubscriber("", 12345),
		exit: make(chan bool),
	}

	err := startListening(client)
	require.Error(t, err)
}
