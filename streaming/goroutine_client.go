package streaming

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/smallnest/chanx"
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

	handlerLooper := t.initHandlerLooper(queue, req)

	topicStr, err := t.getTopicFromServer(req.Address, req.TableName, req.ActionName)
	if err != nil {
		fmt.Printf("Failed to get topic from server: %s\n", err.Error())
		return err
	}

	t.handlerLoppers.Store(topicStr, handlerLooper)

	return nil
}

func (t *GoroutineClient) initHandlerLooper(queue *chanx.UnboundedChan, req *SubscribeRequest) *handlerLopper {
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

	return handlerLooper
}

// UnSubscribe helps you to unsubscribe the specific action of the table according to the req.
func (t *GoroutineClient) UnSubscribe(req *SubscribeRequest) error {
	err := t.unSubscribe(req)
	if err != nil {
		fmt.Printf("UnSubscribe Failed: %s\n", err.Error())
		return err
	}

	topicStr, err := t.stopHandlerLopper(req.Address, req.TableName, req.ActionName)
	if err != nil {
		return err
	}
	t.handlerLoppers.Delete(topicStr)

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

	select {
	case <-t.exit:
	default:
		close(t.exit)
	}
}

func (t *GoroutineClient) doReconnect(s *site) bool {
	topic, err := t.stopHandlerLopper(s.address, s.tableName, s.actionName)
	if err != nil {
		return false
	}

	isSuccess := t.reSubscribe(topic, s)
	if !isSuccess {
		return isSuccess
	}

	fmt.Printf("%s %s Successfully reconnected and subscribed.\n", time.Now().UTC().String(), topic)
	return true
}

func (t *GoroutineClient) reSubscribe(topic string, s *site) bool {
	err := t.Subscribe(transSiteToNewSubscribeRequest(s))
	if err != nil {
		fmt.Printf("%s %s Unable to subscribe to the table. Try again after 1 second.\n", time.Now().UTC().String(), topic)
		return false
	}

	return true
}

func transSiteToNewSubscribeRequest(s *site) *SubscribeRequest {
	return &SubscribeRequest{
		Address:    s.address,
		TableName:  s.tableName,
		ActionName: s.actionName,
		Handler:    s.handler,
		Offset:     s.msgID + 1,
		Filter:     s.filter,
		Reconnect:  s.reconnect,
	}
}

func (t *GoroutineClient) stopHandlerLopper(address, tableName, actionName string) (string, error) {
	topic, err := t.getTopicFromServer(address, tableName, actionName)
	if err != nil {
		fmt.Printf("Failed to get topic from server during reconnection using doReconnect: %s\n", err.Error())
		return "", err
	}

	raw, ok := t.handlerLoppers.Load(topic)
	if !ok || raw == nil {
		fmt.Println("Goroutine for subscription is not started")
		return "", errors.New("Goroutine for subscription is not started")
	}

	handlerLopper := raw.(*handlerLopper)

	handlerLopper.stop()
	return topic, nil
}
