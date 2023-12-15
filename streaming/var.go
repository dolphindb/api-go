package streaming

import "sync"

const (
	// DefaultPort is default listening port.
	DefaultPort = 8848
	localhost   = "localhost"
)

var (
	haTopicToTrueTopic = sync.Map{}
	waitReconnectTopic = sync.Map{}

	messageCache     = sync.Map{}
	trueTopicToSites = sync.Map{}
	queueMap         = sync.Map{}
	reconnectTable   = sync.Map{}
)
