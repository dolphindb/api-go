package streaming

import (
	"fmt"
	"time"
)

// PollingClient is a client for streaming subscription, which allows you to get subscription from the topicPoller.
type PollingClient struct {
	*subscriber

	topicPoller *TopicPoller

	exit chan bool
}

// NewPollingClient instantiates a new polling client and listens on the listening port to get subscription.
func NewPollingClient(listeningHost string, listeningPort int) *PollingClient {
	if listeningPort == 0 {
		listeningPort = DefaultPort
	}

	t := &PollingClient{
		subscriber: newSubscriber(listeningHost, listeningPort),
		exit:       make(chan bool),
	}

	go listening(t, listeningPort)
	return t
}

// Subscribe helps you to subscribe the specific action of the table according to the req.
func (t *PollingClient) Subscribe(req *SubscribeRequest) (*TopicPoller, error) {
	err := t.subscribe(req)
	if err != nil {
		fmt.Printf("Failed to subscribe topic: %s\n", err.Error())
		return nil, err
	}

	return t.topicPoller, nil
}

func (t *PollingClient) subscribe(req *SubscribeRequest) error {
	queue, err := t.subscribeInternal(req)
	if err != nil {
		return err
	}

	t.topicPoller = &TopicPoller{
		queue: queue,
	}

	return nil
}

// UnSubscribe helps you to unsubscribe the specific action of the table according to the req.
func (t *PollingClient) UnSubscribe(req *SubscribeRequest) error {
	return t.unSubscribe(req)
}

// Close closes the client.
func (t *PollingClient) Close() {
	close(t.exit)
}

func (t *PollingClient) getSubscriber() *subscriber {
	return t.subscriber
}

// IsClosed checks whether the client is closed.
func (t *PollingClient) IsClosed() bool {
	select {
	case <-t.exit:
		return true
	default:
		return false
	}
}

func (t *PollingClient) doReconnect(s *site) bool {
	time.Sleep(1 * time.Second)

	queue, err := t.subscribeInternal(transSiteToNewSubscribeRequest(s))
	if err != nil {
		fmt.Printf("%s Unable to subscribe to the table. Try again after 1 second.\n", time.Now().UTC().String())
		return false
	}

	t.topicPoller.queue = queue

	fmt.Printf("%s Successfully reconnected and subscribed.\n", time.Now().UTC().String())
	return true
}
