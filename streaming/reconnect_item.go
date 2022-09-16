package streaming

type reconnectItem struct {
	reconnectState         int
	lastReconnectTimestamp int64
	topics                 []string
}

func (r *reconnectItem) getState() int {
	return r.reconnectState
}

func (r *reconnectItem) setState(state int) *reconnectItem {
	r.reconnectState = state
	return r
}

func (r *reconnectItem) getTimeStamp() int64 {
	return r.lastReconnectTimestamp
}

func (r *reconnectItem) setTimeStamp(stamp int64) {
	r.lastReconnectTimestamp = stamp
}

func (r *reconnectItem) putTopic(topic string) {
	if r.topics == nil {
		r.topics = make([]string, 1)
		r.topics[0] = topic
	} else if !contains(r.topics, topic) {
		r.topics = append(r.topics, topic)
	}
}
