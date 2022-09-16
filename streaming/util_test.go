package streaming

import (
	"testing"

	"github.com/smallnest/chanx"
	"github.com/stretchr/testify/assert"
)

func TestUtil(t *testing.T) {
	src := []string{"sample", "test"}
	assert.Equal(t, contains(src, "sample"), true)
	assert.Equal(t, contains(src, "example"), false)

	msg := &Message{
		topic:  "topic,sample",
		offset: -1,
	}

	haTopicToTrueTopic.Store("topic", "topic")
	queueMap.Store("topic", chanx.NewUnboundedChan(2))

	dispatch(msg)
	raw, ok := queueMap.Load("topic")
	assert.Equal(t, ok, true)

	q := raw.(*chanx.UnboundedChan)
	r := <-q.Out
	m := r.(IMessage)
	assert.Equal(t, m.GetOffset(), int64(-1))
	assert.Equal(t, m.GetTopic(), "topic,sample")

	batchDispatch([]IMessage{msg})
	r = <-q.Out
	m = r.(IMessage)
	assert.Equal(t, m.GetOffset(), int64(-1))
	assert.Equal(t, m.GetTopic(), "topic,sample")

	item := &reconnectItem{
		reconnectState: 1,
	}

	item.putTopic("127.0.0.1:3000:local3000/sub/action1")
	reconnectTable.Store("127.0.0.1:3000:local3000", item)
	setReconnectTimestamp("127.0.0.1:3000:local3000", 10)
	raw, ok = reconnectTable.Load("127.0.0.1:3000:local3000")
	assert.Equal(t, ok, true)

	ri := raw.(*reconnectItem)
	assert.Equal(t, ri.getTimeStamp(), int64(10))
	assert.Equal(t, ri.getState(), 1)

	b := IsClosed("topic")
	assert.Equal(t, b, true)

	sites := []*site{
		{
			tableName: "util",
			closed:    false,
		},
	}

	trueTopicToSites.Store("topic", sites)

	b = IsClosed("topic")
	assert.Equal(t, b, false)
}
