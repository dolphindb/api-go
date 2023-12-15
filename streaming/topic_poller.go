package streaming

import (
	"fmt"
	"time"
)

// TopicPoller is used to take one or more subscription info for polling client.
type TopicPoller struct {
	queue *UnboundedChan
	cache []IMessage

	MsgDeserializer *StreamDeserializer
	msgAsTable bool
}

// Poll retrieves and removes the head of this queue, waiting up to the specified
// wait time if necessary for an element to become available.
func (t *TopicPoller) Poll(timeout, size int) []IMessage {
	retMsgSlice := make([]IMessage, 0, len(t.cache))
	copy(retMsgSlice, t.cache)
	t.cache = make([]IMessage, 0)
	end := time.Now().Add(time.Duration(timeout) * time.Millisecond)
	for len(retMsgSlice) < size && time.Now().Before(end) {
		select {
		case v := <-t.queue.Out:
			if v != nil {
				retMsgSlice = append(retMsgSlice, v.(IMessage))
			}
		default:
			continue
		}
	}
	if t.msgAsTable {
		tbl, err := mergeIMessage(retMsgSlice)
		if err != nil {
			fmt.Printf("merge msg to table failed: %s\n", err.Error());
			return make([]IMessage, 0)
		}
		retMsgSlice = []IMessage{tbl}
	} else if(t.MsgDeserializer != nil) {
		outMsg := make([]IMessage, 0)
		for _, v := range retMsgSlice {
			ret, err := t.MsgDeserializer.Parse(v)
			if err != nil {
				fmt.Printf("StreamDeserializer parse failed: %s\n", err.Error())
			} else {
				outMsg = append(outMsg, ret)
			}
		}
		retMsgSlice = outMsg
	}

	return retMsgSlice
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
