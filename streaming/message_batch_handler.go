package streaming

// MessageBatchHandler is an interface which will help you to handle message.
// You can implement the interface and use it when instantiate a SubscribeRequest or use DefaultMessageBatchHandler by default.
type MessageBatchHandler interface {
	// DoEvent will be called when you subscribe with a goroutineClient or a goroutinePooledClient
	DoEvent(msg []IMessage)
}

// // DefaultMessageBatchHandler is an implementation of MessageBatchHandler, which is the default handler of IMessage.
// type DefaultMessageBatchHandler struct {
// 	start     bool
// 	startTime int64
// 	count     int64
// }

// // DoEvent will be called when you subscribe with a goroutineClient or a goroutinePooledClient.
// func (d *DefaultMessageBatchHandler) DoEvent(msg IMessage) {
	// if !d.start {
	// 	d.start = true
	// 	d.startTime = time.Now().Unix()
	// }

	// atomic.AddInt64(&d.count, 1)
	// fmt.Printf("Get %d messages now.\n", d.count)
	// if d.count%100000 == 0 {
	// 	end := time.Now().Unix()
	// 	fmt.Printf("%d messages took %d ms total, through: %d messages/s\n",
	// 		d.count, end-d.startTime, d.count/(end-d.startTime))
	// }

	// if d.count == 2000000 {
	// 	fmt.Println("Done")
	// }
// }
