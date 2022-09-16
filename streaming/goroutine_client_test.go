package streaming

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var failedAction = "failedAction"

func TestGoroutineClient(t *testing.T) {
	tc := NewGoroutineClient(localhost, 3888)

	req := &SubscribeRequest{
		Address:    testAddr,
		TableName:  "threaded",
		ActionName: "action1",
		Offset:     0,
		Reconnect:  true,
	}

	req.SetBatchSize(10).SetThrottle(1)

	err := tc.Subscribe(req)
	assert.Nil(t, err)

	sendOneSubscription(3888)

	sub := tc.getSubscriber()
	assert.Equal(t, sub.listeningPort, int32(3888))
	assert.Equal(t, strings.HasPrefix(sub.listeningHost, "127.0.0.1"), true)

	item := &reconnectItem{
		reconnectState:         1,
		lastReconnectTimestamp: time.Now().UnixNano() / 1000000,
	}

	item.putTopic("127.0.0.1:3000:local3000/threaded/action1")
	reconnectTable.Store("127.0.0.1:3000:local3000", item)

	time.Sleep(4 * time.Second)

	err = tc.UnSubscribe(req)
	assert.Nil(t, err)

	req.ActionName = failedAction
	err = tc.Subscribe(req)
	assert.Equal(t, err.Error(), "client error response. @K")
	tc.Close()

	time.Sleep(2 * time.Second)
}
