package streaming

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/smallnest/chanx"
)

// GoroutinePooledClient is an implementation of AbstractClient for streaming subscription.
type GoroutinePooledClient struct {
	*subscriber

	exit chan bool

	queueHandlers sync.Map
}

type queueHandlerBinder struct {
	queue   *chanx.UnboundedChan
	handler MessageHandler
}

// NewGoroutinePooledClient instantiates an instance of GoroutinePooledClient,
// which is used to listen on the listening port to receive subscription info.
// When listeningHost is "", the default host is the local address.
// When listeningPort is 0, the default port is the 8849.
// When listeningPort is -1, enable the reverse stream subscription.
func NewGoroutinePooledClient(listeningHost string, listeningPort int) *GoroutinePooledClient {
	if listeningPort == 0 {
		listeningPort = DefaultPort
	}

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

	queueHandler := &queueHandlerBinder{
		queue:   queue,
		handler: req.Handler,
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
	if err := t.unSubscribe(req); err != nil {
		fmt.Printf("UnSubscribe Failed: %s\n", err.Error())
		return err
	}

	topicStr, err := t.getTopicFromServer(req.Address, req.TableName, req.ActionName)
	if err != nil {
		fmt.Printf("Failed to get topic from server: %s\n", err.Error())
		return err
	}

	t.queueHandlers.Delete(topicStr)

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
	t.queueHandlers = sync.Map{}
	close(t.exit)
}

func (t *GoroutinePooledClient) doReconnect(s *site) bool {
	topicStr := fmt.Sprintf("%s/%s/%s", s.address, s.tableName, s.actionName)

	if err := t.Subscribe(transSiteToNewSubscribeRequest(s)); err != nil {
		fmt.Printf("%s %s Unable to subscribe to the table. Try again after 1 second.\n", time.Now().UTC().String(), topicStr)
		return false
	}

	fmt.Printf("%s %s Successfully reconnected and subscribed.\n", time.Now().UTC().String(), topicStr)
	return true
}

func (t *GoroutinePooledClient) run() {
	backLog := chanx.NewUnboundedChan(10)

	for !t.IsClosed() {
	loop:
		for {
			select {
			case val := <-backLog.Out:
				msg := val.(IMessage)
				raw, ok := t.queueHandlers.Load(msg.GetTopic())
				if !ok {
					continue
				}

				binder := raw.(*queueHandlerBinder)

				go binder.handler.DoEvent(msg)

			default:
				if backLog.Len() == 0 && backLog.BufLen() == 0 {
					break loop
				}
			}
		}

		t.refill(backLog)
	}
}

func (t *GoroutinePooledClient) refill(backLog *chanx.UnboundedChan) {
	count := 200
	for !t.fillBackLog(backLog) {
		if count < 100 {
			runtime.Gosched()
		}

		count--
	}
}

func (t *GoroutinePooledClient) fillBackLog(backLog *chanx.UnboundedChan) bool {
	filled := false
	t.queueHandlers.Range(func(k, v interface{}) bool {
		val := v.(*queueHandlerBinder)
		msg := poll(val.queue)
		if len(msg) > 0 {
			for _, val := range msg {
				backLog.In <- val
			}

			filled = true
		}

		return true
	})

	return filled
}
