package streaming

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// PollingClient is a client for streaming subscription, which allows you to get subscription from the topicPoller.
type PollingClient struct {
	*subscriber

	topicPollerMap sync.Map

	exit chan bool
}

// NewPollingClient instantiates a new polling client and listens on the listening port to get subscription.
// When listeningHost is "", the default host is the local address.
// When listeningPort is 0, enable the reverse stream subscription.
func NewPollingClient(listeningHost string, listeningPort int) *PollingClient {
	t := &PollingClient{
		subscriber: newSubscriber(listeningHost, listeningPort),
		topicPollerMap: sync.Map{},
		exit:       make(chan bool),
	}

	return t
}

// Subscribe helps you to subscribe the specific action of the table according to the req.
func (t *PollingClient) Subscribe(req *SubscribeRequest) (*TopicPoller, error) {
	if (req.MsgAsTable) {
		if(req.MsgDeserializer != nil) {
			return nil, errors.New("if MsgAsTable is true, MsgDeserializer must be nil")
		}
	}
	err := t.subscribe(req)
	if err != nil {
		fmt.Printf("Failed to subscribe topic: %s\n", err.Error())
		return nil, err
	}

	topicStr, err := t.getTopicFromServer(req.Address, req.TableName, req.ActionName)
	if err != nil {
		fmt.Printf("Failed to get topic from server: %s\n", err.Error())
		return nil, err
	}
	fmt.Println("subscribe topic: ", topicStr)
	retPoller, ok := t.topicPollerMap.Load(topicStr)
	if !ok {
		return nil, errors.New("Failed to load new poller by topic: " + topicStr)
	}

	return retPoller.(*TopicPoller), nil
}

func (t *PollingClient) subscribe(req *SubscribeRequest) error {
	err := t.reviseSubscriber(req)
	if err != nil {
		return err
	}

	queue, err := t.subscribeInternal(req)
	if err != nil {
		return err
	}

	topicStr, err := t.getTopicFromServer(req.Address, req.TableName, req.ActionName)
	if err != nil {
		fmt.Printf("Failed to get topic from server: %s\n", err.Error())
		return err
	}

	t.topicPollerMap.Store(topicStr, &TopicPoller{
		queue: queue,
		MsgDeserializer: req.MsgDeserializer,
		msgAsTable: req.MsgAsTable,
	})

	return nil
}

func (t *PollingClient) reviseSubscriber(req *SubscribeRequest) error {
	var err error
	t.subscriber.once.Do(func() {
		err = t.subscriber.checkServerVersion(req.Address)
		if err == nil {
			go listening(t)
		}
	})

	return err
}

// UnSubscribe helps you to unsubscribe the specific action of the table according to the req.
func (t *PollingClient) UnSubscribe(req *SubscribeRequest) error {
    topicStr, err := t.getTopicFromServer(req.Address, req.TableName, req.ActionName)
	if err != nil {
		return err
	}
	t.topicPollerMap.Delete(topicStr)

	return t.unSubscribe(req)
}

// Close closes the client.
func (t *PollingClient) Close() {
	// t.topicPollerMap.Range(func(k, v interface{}) bool {
	// 	haTopicToTrueTopic.Delete(k)
	// 	trueTopicToSites.Delete(k)
	// 	queueMap.Delete(k)
	// 	close(v.(*TopicPoller).queue.In)
	// 	return true
	// })

	close(t.connList.In)
	t.topicPollerMap = sync.Map{}
	select {
	case <-t.exit:
	default:
		close(t.exit)
	}
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
	// time.Sleep(1 * time.Second)

	topic, err := t.getTopicFromServer(s.address, s.tableName, s.actionName)
	if err != nil {
		return false
	}
	_, ok := t.topicPollerMap.Load(topic)
	if !ok {
		// HACK no such topic means subscription not exist!, return true to ignore
		raw, ok := queueMap.Load(topic)
		if ok && raw != nil {
			q := raw.(*UnboundedChan)

			closeUnboundedChan(q)
			queueMap.Delete(topic)
			haTopicToTrueTopic.Delete(topic)
			trueTopicToSites.Delete(topic)
		}
		return true
	}
	err = t.reSubscribeInternal(transSiteToNewSubscribeRequest(s))
	if err != nil {
		fmt.Printf("%s Unable to subscribe to the table. Try again after 1 second.\n", time.Now().UTC().String())
		return false
	}

	// topicStr, err := t.getTopicFromServer(s.address, s.tableName, s.actionName)
	// if err != nil {
	// 	fmt.Printf("Failed to get topic from server: %s\n", err.Error())
	// 	return false
	// }
	// retPoller, ok := t.topicPollerMap.Load(topicStr)
	// if !ok {
	// 	fmt.Printf("Failed to load new poller by topic: %s\n", topicStr)
	// 	return false
	// }
	// retPoller.(*TopicPoller).queue = queue

	fmt.Printf("%s Successfully reconnected and subscribed.\n", time.Now().UTC().String())
	return true
}
