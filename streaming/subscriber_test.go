package streaming

import (
	"sync"
	"testing"

	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/stretchr/testify/assert"
)

func TestSubscriber(t *testing.T) {
	later := isLater("2.00.10", "2.00.9")
	assert.True(t, later)

	later = isLater("2.00.8", "2.00.9")
	assert.False(t, later)
}

func TestGetStreamFailoverActionWithNotLeader(t *testing.T) {
	action, ok := getStreamFailoverAction(&dialer.ServerError{
		Code:    dialer.ServerErrNotLeader,
		Raw:     "client error response. <NotLeader>192.168.0.69:8803:dnode2",
		Detail:  "<NotLeader>192.168.0.69:8803:dnode2",
		Address: "192.168.0.69:8803",
	}, "192.168.0.68:8803")

	assert.True(t, ok)
	assert.Equal(t, "192.168.0.69:8803", action.address)
	assert.Contains(t, action.friendlyMessage, "192.168.0.68:8803")
	assert.Contains(t, action.friendlyMessage, "Streaming subscription is unavailable")
	assert.Contains(t, action.friendlyMessage, "not the current raft leader")
	assert.Contains(t, action.friendlyMessage, "192.168.0.69:8803")
}

func TestGetStreamFailoverActionWithUnknownLeader(t *testing.T) {
	action, ok := getStreamFailoverAction(&dialer.ServerError{
		Code:   dialer.ServerErrUnknownLeader,
		Raw:    "client error response. <UnknownLeader>leader is temporarily unavailable",
		Detail: "<UnknownLeader>leader is temporarily unavailable",
	}, "192.168.0.68:8803")

	assert.True(t, ok)
	assert.Empty(t, action.address)
	assert.Contains(t, action.friendlyMessage, "Streaming subscription is temporarily unavailable")
	assert.Contains(t, action.friendlyMessage, "192.168.0.68:8803")
	assert.Contains(t, action.friendlyMessage, "temporarily unavailable")
}

func TestNextFailoverRequestSkipsCurrentAddress(t *testing.T) {
	oldRequests := trueTopicToRequests
	t.Cleanup(func() {
		trueTopicToRequests = oldRequests
	})

	trueTopicToRequests = syncMapWithTopic("topic", []*SubscribeRequest{
		{Address: "192.168.0.68:8803"},
		{Address: "192.168.0.69:8803"},
	})

	req := (&subscriber{}).nextFailoverRequest("topic", "192.168.0.68:8803")

	assert.NotNil(t, req)
	assert.Equal(t, "192.168.0.69:8803", req.Address)
}

func syncMapWithTopic(topic string, requests []*SubscribeRequest) sync.Map {
	m := sync.Map{}
	m.Store(topic, requests)
	return m
}
