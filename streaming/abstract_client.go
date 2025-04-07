package streaming

import (
	"net"
)

// AbstractClient is the client interface for streaming subscription.
type AbstractClient interface {
	activeCloseConnection(req *SubscribeRequest) error
	doReconnect(req *SubscribeRequest) bool
	getSubscriber() *subscriber
	getConn() (net.Conn, bool)

	subscribe(req *SubscribeRequest) error
	// UnSubscribe helps you to unsubscribe the specific action of the table according to the req
	UnSubscribe(req *SubscribeRequest) error
	// IsClose checks whether the client is close
	IsClosed() bool
}
