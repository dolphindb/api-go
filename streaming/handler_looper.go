package streaming

import (
	"time"

	"github.com/smallnest/chanx"
)

type handlerLopper struct {
	queue   *chanx.UnboundedChan
	handler MessageHandler

	batchSize *int
	throttle  *int
	exit      chan bool
}

func (h *handlerLopper) getThrottle() int {
	if h.throttle == nil {
		return -1
	}

	return *h.throttle
}

func (h *handlerLopper) getBatchSize() int {
	if h.batchSize == nil {
		return -1
	}

	return *h.batchSize
}

func (h *handlerLopper) stop() {
	select {
	case <-h.exit:
	default:
		close(h.exit)
	}
}

func (h *handlerLopper) run() {
	h.exit = make(chan bool)
	for {
		select {
		case <-h.exit:
			return
		default:
			h.handleMessage()
		}
	}
}

func (h *handlerLopper) handleMessage() {
	msg := make([]IMessage, 0)
	batchSize := h.getBatchSize()
	throttle := h.getThrottle()
	switch {
	case batchSize == -1 && throttle == -1:
		v := <-h.queue.Out
		msg = append(msg, v.(IMessage))
	case batchSize != -1 && throttle != -1:
		end := time.Now().Add(time.Duration(throttle) * time.Millisecond)
		for len(msg) == 0 || ((len(msg) == 0 || len(msg) < batchSize) && time.Now().Before(end)) {
			tmp := poll(h.queue)
			if tmp != nil {
				if msg == nil {
					msg = tmp
				} else {
					msg = append(msg, tmp...)
				}
			}
		}
	default:
		end := time.Now().Add(time.Duration(throttle) * time.Millisecond)
		for len(msg) == 0 || time.Now().Before(end) {
			tmp := poll(h.queue)
			if tmp != nil {
				if msg == nil {
					msg = tmp
				} else {
					msg = append(msg, tmp...)
				}
			}
		}
	}

	for _, v := range msg {
		h.handler.DoEvent(v)
	}
}

func poll(queue *chanx.UnboundedChan) []IMessage {
	res := make([]IMessage, 0)
	for {
		select {
		case val := <-queue.Out:
			res = append(res, val.(IMessage))
		default:
			return res
		}
	}
}
