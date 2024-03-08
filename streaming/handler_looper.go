package streaming

import (
	"errors"
	"fmt"
	"time"

	"github.com/dolphindb/api-go/model"
)

type handlerLopper struct {
	queue   *UnboundedChan
	handler MessageHandler
	batchHandler MessageBatchHandler
	MsgDeserializer *StreamDeserializer

	msgAsTable bool
	batchSize *int
	throttle  *int
	exit      chan bool
	affirmExit chan bool
}

func (h *handlerLopper) getThrottle() int {
	if h.throttle == nil {
		return 1000
	} else if *h.throttle <= 0 {
		return -1
	}

	return *h.throttle
}

func (h *handlerLopper) getBatchSize() int {
	if h.batchSize == nil || *h.batchSize < 1 {
		return -1
	}

	return *h.batchSize
}

func (h *handlerLopper) isStopped() bool {
	select {
	case <-h.exit:
		return true
	default:
		return false
	}
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

func mergeIMessage(msg []IMessage) (IMessage, error) {
	firstMsg := msg[0].(*Message)
	colNum := len(firstMsg.nameToIndex)
	colNames := make([]string, colNum)
	colValues := make([](*model.Vector), colNum)
	for k,v := range firstMsg.nameToIndex {
		colNames[v] = k
		val := firstMsg.GetValue(v)
		valType := val.(*model.Scalar).DataType
		dataTypeList := []model.DataType{valType}
		dataList := model.NewDataTypeList(val.GetDataType(), dataTypeList)
		colValues[v] = model.NewVector(dataList)
	}

	for i := 1; i < len(msg); i++ {
		inMsg := msg[i].(*Message)
		for k,v := range inMsg.nameToIndex {
			colNames[v] = k
			val := inMsg.GetValue(v)
			valType := val.(*model.Scalar).DataType
			err := colValues[v].Append(valType)
			if err != nil {
				return nil, errors.New("merge IMessage to table failed, due to " + err.Error())
			}
		}
	}
	table := model.NewTable(colNames, colValues)
	ret := &TableMessage {
		offset: firstMsg.offset,
		topic: firstMsg.topic,
		sym: firstMsg.sym,

		msg: table,
	}
	return ret, nil
}

func (h *handlerLopper) handleMessage() {
	msg := h.collectMessage()
	if len(msg) == 0 {
		return
	}
	if(h.msgAsTable) {
		ret, err := mergeIMessage(msg)
		if err != nil {
			fmt.Printf("merge msg to table failed: %s\n", err.Error())
		}
		if !h.isStopped() {
			h.handler.DoEvent(ret)
		}
	} else if (h.batchSize != nil && *h.batchSize >= 1) {
		if(h.MsgDeserializer != nil) {
			outMsg := make([]IMessage, 0)
			for _, v := range msg {
				ret, err := h.MsgDeserializer.Parse(v)
				if err != nil {
					fmt.Printf("StreamDeserializer parse failed: %s\n", err.Error())
				} else {
					outMsg = append(outMsg, ret)
				}
			}
			if !h.isStopped() {
				h.batchHandler.DoEvent(outMsg)
			}
		} else {
			if !h.isStopped() {
				h.batchHandler.DoEvent(msg)
			}
		}
	} else {
		for _, v := range msg {
			if(h.MsgDeserializer != nil) {
				ret, err := h.MsgDeserializer.Parse(v)
				if err != nil {
					fmt.Printf("StreamDeserializer parse failed: %s\n", err.Error())
				} else {
					if !h.isStopped() {
						h.handler.DoEvent(ret)
					}
				}
			} else {
				if !h.isStopped() {
					h.handler.DoEvent(v)
				}
			}
		}
	}
}

func (h *handlerLopper) collectMessage() []IMessage {
	batchSize := h.getBatchSize()
	throttle := h.getThrottle()
	return batchPoll(h.queue, batchSize, throttle)
}

func isTimeout(t time.Time) bool {
	return !time.Now().Before(t)
}

func batchPoll(queue *UnboundedChan, batchSize int, throttle int) []IMessage {
	msg := make([]IMessage, 0)
	switch {
	case batchSize == -1 && throttle == -1:
		v := <-queue.Out
		msg = append(msg, v.(IMessage))
	case batchSize != -1 && throttle != -1:
		end := time.Now().Add(time.Duration(throttle) * time.Millisecond)
		for len(msg) < batchSize && !isTimeout(end) {
			select {
			case val := <-queue.Out:
				// HACK don't know why would get nil
				if val == nil { // TODO add more val test
					continue
				}
				msg = append(msg, val.(IMessage))
			default:
				continue
			}
		}
	default:
		defaultLen := 1
		if batchSize != -1 {
			defaultLen = batchSize
		}
		for len(msg) < defaultLen {
			select {
			case val := <-queue.Out:
				msg = append(msg, val.(IMessage))
			default:
				return msg
			}
		}
	}


	return msg;
}

// func poll(queue *UnboundedChan, batchSize int, throttle int) []IMessage {
// 	res := make([]IMessage, 0)
// 	for {
// 		select {
// 		case val := <-queue.Out:
// 			res = append(res, val.(IMessage))
// 		default:
// 			return res
// 		}
// 	}
// }
