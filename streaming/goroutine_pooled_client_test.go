package streaming

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGoroutinePooledClient(t *testing.T) {
	tpc := NewGoroutinePooledClient(localhost, 2888)

	req := &SubscribeRequest{
		Address:    testAddr,
		TableName:  "pooled",
		ActionName: "action1",
		Offset:     0,
		Reconnect:  true,
	}

	req.SetBatchSize(10).SetThrottle(1)

	err := tpc.Subscribe(req)
	assert.Nil(t, err)

	sendMoreSubscription(2888)

	sub := tpc.getSubscriber()
	assert.Equal(t, sub.listeningPort, int32(2888))
	assert.Equal(t, strings.HasPrefix(sub.listeningHost, "127.0.0.1"), true)

	item := &reconnectItem{
		reconnectState:         1,
		lastReconnectTimestamp: time.Now().UnixNano() / 1000000,
	}

	item.putTopic("127.0.0.1:3000:local3000/pooled/action1")
	reconnectTable.Store("127.0.0.1:3000:local3000", item)

	time.Sleep(3 * time.Second)

	err = tpc.UnSubscribe(req)
	assert.Nil(t, err)

	req.ActionName = failedAction
	err = tpc.Subscribe(req)
	assert.Equal(t, err.Error(), "client error response. @K")
	tpc.Close()
}
