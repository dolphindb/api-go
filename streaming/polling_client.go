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
	t.stop()
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

	req := &SubscribeRequest{
		Address:     s.address,
		TableName:   s.tableName,
		ActionName:  s.actionName,
		Handler:     s.handler,
		Offset:      s.msgID + 1,
		Filter:      s.filter,
		Reconnect:   s.reconnect,
		AllowExists: s.AllowExists,
	}

	queue, err := t.subscribeInternal(req)
	if err != nil {
		fmt.Printf("%s Unable to subscribe to the table. Try again after 1 second.\n", time.Now().UTC().String())
		return false
	}

	t.topicPoller.queue = queue

	fmt.Printf("%s Successfully reconnected and subscribed.\n", time.Now().UTC().String())
	return true
}

func (t *PollingClient) stop() {
	select {
	case <-t.exit:
	default:
		close(t.exit)
	}
}

func (t *PollingClient) tryReconnect(topic string) bool {
	topicRaw, ok := haTopicToTrueTopic.Load(topic)
	if !ok {
		return false
	}

	queueMap.Delete(topicRaw)

	raw, ok := trueTopicToSites.Load(topicRaw)
	if !ok {
		return false
	}

	sites := raw.([]*site)

	if len(sites) == 0 {
		return false
	}

	if len(sites) == 1 && !sites[0].reconnect {
		return false
	}

	site := getActiveSite(sites)
	if site != nil {
		if t.doReconnect(site) {
			waitReconnectTopic.Delete(topicRaw)
			return true
		}

		waitReconnectTopic.Store(topicRaw, topicRaw)
		return false
	}

	return false
}
