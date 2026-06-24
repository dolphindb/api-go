package streaming

import (
	"errors"
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
// When listeningPort is 0, enable the reverse stream subscription.
func NewGoroutineClient(listeningHost string, listeningPort int) *GoroutineClient {
	t := &GoroutineClient{
		subscriber:     newSubscriber(listeningHost, listeningPort),
		exit:           make(chan bool),
		handlerLoppers: sync.Map{},
	}

	return t
}

// Subscribe helps you to subscribe the specific action of the table according to the req.
func (t *GoroutineClient) Subscribe(req *SubscribeRequest) error {
	if req.MsgAsTable {
		if req.MsgDeserializer != nil {
			return errors.New("if MsgAsTable is true, MsgDeserializer must be nil")
		}
		if req.Handler == nil {
			return errors.New("if MsgAsTable is true, the callback in Handler will be called, so it shouldn't be nil")
		}
	} else {
		if req.BatchSize != nil && *req.BatchSize >= 1 {
			if !req.MsgAsTable && req.BatchHandler == nil {
				return errors.New("if BatchSize >= 1 and MsgAsTable is false, the callback in BatchHandler will be called, so it shouldn't be nil")
			}
		} else {
			if req.BatchSize == nil && req.Handler == nil {
				return errors.New("if BatchSize is not set, the callback in Handler will be called, so it shouldn't be nil")
			}
		}
	}
	return t.subscribe(req)
}

// Subscribe helps you to subscribe the specific action of the table according to the req.
func (t *GoroutineClient) subscribe(req *SubscribeRequest) error {
	err := t.reviseSubscriber(req)
	if err != nil {
		return err
	}

	queue, err := t.subscribeInternal(req)
	if err != nil {
		return err
	}

	handlerLooper := t.initHandlerLooper(queue, req)

	topicStr, err := t.getTopicFromServer(req)
	if err != nil {
		streamingLogErrorf("failed to get topic from server: %v", err)
		return err
	}

	t.handlerLoppers.Store(topicStr, handlerLooper)

	go handlerLooper.run()

	return nil
}

func (t *GoroutineClient) reviseSubscriber(req *SubscribeRequest) error {
	return t.subscriber.initListening(t, req)
}

func (t *GoroutineClient) initHandlerLooper(queue *UnboundedChan, req *SubscribeRequest) *handlerLopper {
	var handlerThrottle *int
	if req.Throttle != nil {
		tmp := int(*req.Throttle * 1000)
		handlerThrottle = &tmp
	} else {
		handlerThrottle = nil
	}
	handlerLooper := &handlerLopper{
		queue:           queue,
		handler:         req.Handler,
		batchHandler:    req.BatchHandler,
		batchSize:       req.BatchSize,
		msgAsTable:      req.MsgAsTable,
		throttle:        handlerThrottle,
		MsgDeserializer: req.MsgDeserializer,
	}

	// if req.Handler == nil {
	// 	handlerLooper.handler = &DefaultMessageHandler{}
	// }

	return handlerLooper
}

// UnSubscribe helps you to unsubscribe the specific action of the table according to the req.
func (t *GoroutineClient) UnSubscribe(req *SubscribeRequest) error {
	topicStr, _, err := t.stopHandlerLopper(req)
	if err != nil {
		return err
	}
	t.handlerLoppers.Delete(topicStr)
	err = t.unSubscribe(req)
	if err != nil {
		streamingLogErrorf("unsubscribe failed: %v", err)
		return err
	}
	// close(looper.queue.In)
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
	// t.handlerLoppers.Range(func(k, v interface{}) bool {
	// 	val := v.(*handlerLopper)
	// 	if !val.isStopped() {
	// 		val.stop()
	// 		haTopicToTrueTopic.Delete(k)
	// 		trueTopicToSites.Delete(k)
	// 		queueMap.Delete(k)
	// 	}
	// 	return true
	// })

	closeUnboundedChan(t.connList)

	select {
	case <-t.exit:
	default:
		close(t.exit)
	}
}

func (t *GoroutineClient) doReconnect(req *SubscribeRequest) bool {
	// topic, err := t.stopHandlerLopper(s.address, s.tableName, s.actionName)
	// if err != nil {
	// 	return false
	// }

	topic, err := t.getTopicFromServer(req)
	if err != nil {
		return false
	}
	_, ok := t.handlerLoppers.Load(topic)
	if !ok {
		// HACK no such topic means subscription not exist!, return true to ignore
		raw, ok := queueMap.Load(topic)
		if ok && raw != nil {
			q := raw.(*UnboundedChan)

			closeUnboundedChan(q)
			queueMap.Delete(topic)
			haTopicToTrueTopic.Delete(topic)
			trueTopicToRequests.Delete(topic)
		}
		return true
	}

	isSuccess := t.reSubscribe(topic, req)
	if !isSuccess {
		return isSuccess
	}

	streamingLogInfof("%s %s successfully reconnected and subscribed", time.Now().UTC().String(), topic)
	return true
}

func (t *GoroutineClient) reSubscribe(topic string, req *SubscribeRequest) bool {
	req.Offset = req.Offset + 1
	err := t.reSubscribeInternal(req)
	if err != nil {
		streamingLogWarnf("%s %s unable to subscribe to the table; try again after 1 second", time.Now().UTC().String(), topic)
		return false
	}

	return true
}

// func transSiteToNewSubscribeRequest(s *site) *SubscribeRequest {
// 	return &SubscribeRequest{
// 		Address:    s.address,
// 		TableName:  s.tableName,
// 		ActionName: s.actionName,
// 		Handler:    s.handler,
// 		Offset:     s.msgID + 1,
// 		Filter:     s.filter,
// 		Reconnect:  s.reconnect,
// 	}
// }

func (t *GoroutineClient) stopHandlerLopper(req *SubscribeRequest) (string, *handlerLopper, error) {
	topic, err := t.getTopicFromServer(req)
	if err != nil {
		streamingLogErrorf("failed to get topic from server during reconnection using doReconnect: %v", err)
		return "", nil, err
	}

	raw, ok := t.handlerLoppers.Load(topic)
	if !ok || raw == nil {
		streamingLogWarnf("goroutine for subscription is not started")
		return "", nil, errors.New("Goroutine for subscription is not started")
	}

	handlerLopper := raw.(*handlerLopper)

	if !handlerLopper.isStopped() {
		handlerLopper.stop()
	}
	return topic, handlerLopper, nil
}
