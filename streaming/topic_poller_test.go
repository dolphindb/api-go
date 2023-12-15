package streaming

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopicPoller(t *testing.T) {
	tp := &TopicPoller{
		queue: NewUnboundedChan(1),
		cache: make([]IMessage, 0),
	}
	var msg IMessage
	go func() {
		msg = tp.Take()
	}()

	tp.queue.In <- &Message{
		offset: 0,
		topic:  "topic",
	}
	//nolint
	for msg == nil {
		// loop
	}

	assert.Equal(t, msg.GetOffset(), int64(0))
	assert.Equal(t, msg.GetTopic(), "topic")

	var ms []IMessage
	go func() {
		ms = tp.Poll(1, 1)
	}()

	tp.queue.In <- &Message{
		offset: 1,
		topic:  "topic1",
	}

	//nolint
	for ms == nil {
		// loop
	}

	assert.Equal(t, ms[0].GetOffset(), int64(1))
	assert.Equal(t, ms[0].GetTopic(), "topic1")
}
