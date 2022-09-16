package streaming

// AbstractClient is the client interface for streaming subscription.
type AbstractClient interface {
	activeCloseConnection(si *site) error
	tryReconnect(topic string) bool
	doReconnect(si *site) bool
	getSubscriber() *subscriber

	subscribe(req *SubscribeRequest) error
	// UnSubscribe helps you to unsubscribe the specific action of the table according to the req
	UnSubscribe(req *SubscribeRequest) error
	// IsClose checks whether the client is close
	IsClosed() bool
}
