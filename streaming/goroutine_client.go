package streaming

import (
	"fmt"
	"sync"
	"time"
)

// GoroutineClient is an implementation of AbstractClient for streaming subscription.
type GoroutineClient struct {
	*subscriber

	exit chan bool

	handlerLoppers sync.Map
}

// NewGoroutineClient instantiates an instance of GoroutineClient, which is used to listen on the listening port to receive subscription info.
// When listeningHost is "", the default host is the local address.
// When listeningPort is 0, the default port is the 8849.
func NewGoroutineClient(listeningHost string, listeningPort int) *GoroutineClient {
	if listeningPort == 0 {
		listeningPort = DefaultPort
	}

	t := &GoroutineClient{
		subscriber:     newSubscriber(listeningHost, listeningPort),
		exit:           make(chan bool),
		handlerLoppers: sync.Map{},
	}

	go listening(t, listeningPort)

	return t
}

// Subscribe helps you to subscribe the specific action of the table according to the req.
func (t *GoroutineClient) Subscribe(req *SubscribeRequest) error {
	return t.subscribe(req)
}

// Subscribe helps you to subscribe the specific action of the table according to the req.
func (t *GoroutineClient) subscribe(req *SubscribeRequest) error {
	queue, err := t.subscribeInternal(req)
	if err != nil {
		return err
	}

	handlerLooper := &handlerLopper{
		queue:     queue,
		handler:   req.Handler,
		batchSize: req.BatchSize,
		throttle:  req.Throttle,
	}

	if req.Handler == nil {
		handlerLooper.handler = &DefaultMessageHandler{}
	}

	go handlerLooper.run()

	conn, err := newConnectedConn(req.Address)
	if err != nil {
		fmt.Printf("Failed to connect to server: %s\n", err.Error())
		return err
	}

	defer conn.Close()

	topicStr, err := t.getTopicFromServer(req.TableName, req.ActionName, conn)
	if err != nil {
		fmt.Printf("Failed to get topic from server: %s\n", err.Error())
		return err
	}

	t.handlerLoppers.Store(topicStr, handlerLooper)

	return nil
}

// UnSubscribe helps you to unsubscribe the specific action of the table according to the req.
func (t *GoroutineClient) UnSubscribe(req *SubscribeRequest) error {
	err := t.unSubscribe(req)
	if err != nil {
		fmt.Printf("UnSubscribe Failed: %s\n", err.Error())
		return err
	}

	conn, err := newConnectedConn(req.Address)
	if err != nil {
		fmt.Printf("Failed to connect to server: %s\n", err.Error())
		return err
	}

	defer conn.Close()

	topicStr, err := t.getTopicFromServer(req.TableName, req.ActionName, conn)
	if err != nil {
		fmt.Printf("Failed to get topic from server: %s\n", err.Error())
		return err
	}

	raw, ok := t.handlerLoppers.Load(topicStr)
	if !ok {
		return nil
	}

	handlerLopper := raw.(*handlerLopper)
	t.handlerLoppers.Delete(topicStr)
	handlerLopper.stop()

	return err
}

func (t *GoroutineClient) getSubscriber() *subscriber {
	return t.subscriber
}

// IsClosed checks whether the client is closed.
func (t *GoroutineClient) IsClosed() bool {
	select {
	case <-t.exit:
		return true
	default:
		return false
	}
}

// Close closes the client and stop subscribing.
func (t *GoroutineClient) Close() {
	t.handlerLoppers.Range(func(k, v interface{}) bool {
		val := v.(*handlerLopper)

		val.stop()
		return true
	})

	t.handlerLoppers = sync.Map{}
	t.stop()
}

func (t *GoroutineClient) doReconnect(s *site) bool {
	conn, err := newConnectedConn(s.address)
	if err != nil {
		fmt.Printf("Failed to connect to server: %s\n", err.Error())
		return false
	}

	topic, err := t.getTopicFromServer(s.tableName, s.actionName, conn)
	if err != nil {
		fmt.Printf("Failed to get topic from server during reconnection using doReconnect: %s\n", err.Error())
		return false
	}

	raw, ok := t.handlerLoppers.Load(topic)
	if !ok || raw == nil {
		fmt.Println("Goroutine for subscription is not started")
		return false
	}

	handlerLopper := raw.(*handlerLopper)

	handlerLopper.stop()

	req := &SubscribeRequest{
		Address:    s.address,
		TableName:  s.tableName,
		ActionName: s.actionName,
		Handler:    s.handler,
		Offset:     s.msgID + 1,
		Filter:     s.filter,
		Reconnect:  s.reconnect,
	}

	err = t.Subscribe(req)
	if err != nil {
		fmt.Printf("%s %s Unable to subscribe to the table. Try again after 1 second.\n", time.Now().UTC().String(), topic)
		return false
	}

	fmt.Printf("%s %s Successfully reconnected and subscribed.\n", time.Now().UTC().String(), topic)
	return true
}

func (t *GoroutineClient) stop() {
	select {
	case <-t.exit:
	default:
		close(t.exit)
	}
}

func (t *GoroutineClient) tryReconnect(topic string) bool {
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
