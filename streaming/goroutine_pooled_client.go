package streaming

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// GoroutinePooledClient is an implementation of AbstractClient for streaming subscription.
type GoroutinePooledClient struct {
	*subscriber

	exit chan bool

	queueHandlers sync.Map
}

type queueHandlerBinder struct {
	queue   *UnboundedChan
	handler MessageHandler
	batchHandler MessageBatchHandler
	MsgDeserializer *StreamDeserializer
	msgAsTable bool
	batchSize *int
	throttle  *int
}

func (h *queueHandlerBinder) getThrottle() int {
	if h.throttle == nil {
		return 1000
	} else if *h.throttle <= 0 {
		return -1
	}

	return *h.throttle
}

func (h *queueHandlerBinder) getBatchSize() int {
	if h.batchSize == nil || *h.batchSize < 1 {
		return -1
	}

	return *h.batchSize
}

// NewGoroutinePooledClient instantiates an instance of GoroutinePooledClient,
// which is used to listen on the listening port to receive subscription info.
// When listeningHost is "", the default host is the local address.
// When listeningPort is 0, enable the reverse stream subscription.
func NewGoroutinePooledClient(listeningHost string, listeningPort int) *GoroutinePooledClient {
	t := &GoroutinePooledClient{
		subscriber:    newSubscriber(listeningHost, listeningPort),
		exit:          make(chan bool),
		queueHandlers: sync.Map{},
	}

	go t.run()

	return t
}

// Subscribe helps you to subscribe the specific action of the table according to the req.
func (t *GoroutinePooledClient) Subscribe(req *SubscribeRequest) error {
	if (req.MsgAsTable) {
		if(req.MsgDeserializer != nil) {
			return errors.New("if MsgAsTable is true, MsgDeserializer must be nil")
		}
		if(req.Handler == nil) {
			return errors.New("if MsgAsTable is true, the callback in Handler will be called, so it shouldn't be nil")
		}
	} else {
		if(req.BatchSize != nil && *req.BatchSize >= 1) {
			if(!req.MsgAsTable && req.BatchHandler == nil) {
				return errors.New("if BatchSize >= 1 and MsgAsTable is false, the callback in BatchHandler will be called, so it shouldn't be nil")
			}
		} else {
			if(req.BatchSize == nil && req.Handler == nil) {
				return errors.New("if BatchSize is not set, the callback in Handler will be called, so it shouldn't be nil")
			}
		}
	}
	return t.subscribe(req)
}

func (t *GoroutinePooledClient) subscribe(req *SubscribeRequest) error {
	err := t.reviseSubscriber(req)
	if err != nil {
		return err
	}

	queue, err := t.subscribeInternal(req)
	if err != nil {
		fmt.Printf("Failed to subscribe: %s\n", err.Error())
		return err
	}

	topicStr, err := t.getTopicFromServer(req.Address, req.TableName, req.ActionName)
	if err != nil {
		fmt.Printf("Failed to get topic from server: %s\n", err.Error())
		return err
	}
	var queueHandlerThrottle *int
	if req.Throttle != nil {
		tmp := int(*req.Throttle * 1000)
		queueHandlerThrottle = &tmp
	} else {
		queueHandlerThrottle = nil
	}
	queueHandler := &queueHandlerBinder{
		queue:   queue,
		handler: req.Handler,
		batchHandler: req.BatchHandler,
		MsgDeserializer: req.MsgDeserializer,
		msgAsTable: req.MsgAsTable,
		batchSize: req.BatchSize,
		throttle: queueHandlerThrottle,
	}

	if req.Handler == nil {
		queueHandler.handler = &DefaultMessageHandler{}
	}

	t.queueHandlers.Store(topicStr, queueHandler)

	return nil
}

func (t *GoroutinePooledClient) reviseSubscriber(req *SubscribeRequest) error {
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
func (t *GoroutinePooledClient) UnSubscribe(req *SubscribeRequest) error {
	topicStr, err := t.getTopicFromServer(req.Address, req.TableName, req.ActionName)
	if err != nil {
		fmt.Printf("Failed to get topic from server: %s\n", err.Error())
		return err
	}

	t.queueHandlers.Delete(topicStr)

	if err := t.unSubscribe(req); err != nil {
		fmt.Printf("UnSubscribe Failed: %s\n", err.Error())
		return err
	}

	return nil
}

func (t *GoroutinePooledClient) getSubscriber() *subscriber {
	return t.subscriber
}

// IsClosed checks whether the client is closed.
func (t *GoroutinePooledClient) IsClosed() bool {
	select {
	case <-t.exit:
		return true
	default:
		return false
	}
}

// Close closes the client and stop subscribing.
func (t *GoroutinePooledClient) Close() {
	// t.queueHandlers.Range(func(k, v interface{}) bool {
	// 	close(v.(queueHandlerBinder).queue.In)
	// 	haTopicToTrueTopic.Delete(k)
	// 	trueTopicToSites.Delete(k)
	// 	queueMap.Delete(k)
	// 	return true
	// })

	close(t.connList.In)
	t.queueHandlers = sync.Map{}
	select {
	case <-t.exit:
	default:
		close(t.exit)
	}
}

func (t *GoroutinePooledClient) doReconnect(s *site) bool {
	topicStr := fmt.Sprintf("%s/%s/%s", s.address, s.tableName, s.actionName)


	topic, err := t.getTopicFromServer(s.address, s.tableName, s.actionName)
	if err != nil {
		return false
	}
	_, ok := t.queueHandlers.Load(topic)
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
	if err := t.reSubscribeInternal(transSiteToNewSubscribeRequest(s)); err != nil {
		fmt.Printf("%s %s Unable to subscribe to the table. Try again after 1 second.\n", time.Now().UTC().String(), topicStr)
		return false
	}

	fmt.Printf("%s %s Successfully reconnected and subscribed.\n", time.Now().UTC().String(), topicStr)
	return true
}

func (t *GoroutinePooledClient) run() {
	backLog := NewUnboundedChan(10)
	defer close(backLog.In)

	for !t.IsClosed() {
	loop:
		for {
			select {
			case val := <-backLog.Out:
				msg := val.([]IMessage)
				if len(msg) == 0 {
					return
				}
				// HACK not know any other way to get topic of msgs
				raw, ok := t.queueHandlers.Load(msg[0].GetTopic())
				if !ok {
					continue
				}

				binder := raw.(*queueHandlerBinder)

				if(binder.msgAsTable) {
					ret, err := mergeIMessage(msg)
					if err != nil {
						fmt.Printf("merge msg to table failed: %s\n", err.Error())
					}
					go binder.handler.DoEvent(ret)
				} else if (binder.batchSize != nil && *binder.batchSize >= 1) {
					if(binder.MsgDeserializer != nil) {
						outMsg := make([]IMessage, 0)
						for _, v := range msg {
							ret, err := binder.MsgDeserializer.Parse(v)
							if err != nil {
								fmt.Printf("StreamDeserializer parse failed: %s\n", err.Error())
							} else {
								outMsg = append(outMsg, ret)
							}
						}
						go binder.batchHandler.DoEvent(outMsg)
					} else {
						go binder.batchHandler.DoEvent(msg)
					}
				} else {
					for _, v := range msg {
						if(binder.MsgDeserializer != nil) {
							ret, err := binder.MsgDeserializer.Parse(v)
							if err != nil {
								fmt.Printf("StreamDeserializer parse failed: %s\n", err.Error())
							} else {
								go binder.handler.DoEvent(ret)
							}
						} else {
							go binder.handler.DoEvent(v)
						}
					}
				}

			default:
				if backLog.Len() == 0 && backLog.BufLen() == 0 {
					break loop
				}
			}
		}

		t.fillBackLog(backLog)
		// t.refill(backLog)
	}
}

// func (t *GoroutinePooledClient) refill(backLog *UnboundedChan) {
// 	count := 200
// 	for !t.fillBackLog(backLog) && count > 0 {
// 		if count < 100 {
// 			runtime.Gosched()
// 		}

// 		count--
// 	}
// }

func (t *GoroutinePooledClient) fillBackLog(backLog *UnboundedChan) bool {

	filled := false
	t.queueHandlers.Range(func(k, v interface{}) bool {
		val := v.(*queueHandlerBinder)
		batchSize := val.getBatchSize()
		throttle := val.getThrottle()
		msg := batchPoll(val.queue, batchSize, throttle)
		if len(msg) > 0 {
			backLog.In <- msg
			filled = true
		}

		return true
	})

	return filled
}
