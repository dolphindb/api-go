package streaming

import (
	"fmt"
	"sync/atomic"
	"time"
)

// MessageHandler is an interface which will help you to handle message.
// You can implement the interface and use it when instantiate a SubscribeRequest or use DefaultMessageHandler by default.
type MessageHandler interface {
	// DoEvent will be called when you subscribe with a goroutineClient or a goroutinePooledClient
	DoEvent(msg IMessage)
}

// DefaultMessageHandler is an implementation of MessageHandler, which is the default handler of IMessage.
type DefaultMessageHandler struct {
	start     bool
	startTime int64
	count     int64
}

// DoEvent will be called when you subscribe with a goroutineClient or a goroutinePooledClient.
func (d *DefaultMessageHandler) DoEvent(msg IMessage) {
	if !d.start {
		d.start = true
		d.startTime = time.Now().Unix()
	}

	atomic.AddInt64(&d.count, 1)
	fmt.Printf("Get %d messages now.\n", d.count)
	if d.count%100000 == 0 {
		end := time.Now().Unix()
		fmt.Printf("%d messages took %d ms total, through: %d messages/s\n",
			d.count, end-d.startTime, d.count/(end-d.startTime))
	}

	if d.count == 2000000 {
		fmt.Println("Done")
	}
}
