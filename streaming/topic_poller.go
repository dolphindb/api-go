package streaming

import (
	"time"

	"github.com/smallnest/chanx"
)

// TopicPoller is used to take one or more subscription info for polling client.
type TopicPoller struct {
	queue *chanx.UnboundedChan
	cache []IMessage
}

// Poll retrieves and removes the head of this queue, waiting up to the specified
// wait time if necessary for an element to become available.
func (t *TopicPoller) Poll(timeout, size int) []IMessage {
	l := make([]IMessage, 0, len(t.cache))
	copy(l, t.cache)
	t.cache = make([]IMessage, 0)
	end := time.Now().Add(time.Duration(timeout) * time.Millisecond)
	for len(l) < size && time.Now().Before(end) {
		select {
		case v := <-t.queue.Out:
			if v != nil {
				l = append(l, v.(IMessage))
			}
		default:
			continue
		}
	}

	return l
}

// Take retrieves and removes the head of this queue, waiting if necessary until an element becomes available.
func (t *TopicPoller) Take() IMessage {
	for {
		if len(t.cache) > 0 {
			msg := t.cache[0]
			t.cache = t.cache[1:]
			return msg
		}

	loop:
		for {
			select {
			case val := <-t.queue.Out:
				if val != nil {
					t.cache = append(t.cache, val.(IMessage))
				}
			default:
				if len(t.cache) > 0 {
					break loop
				}
			}
		}
	}
}
