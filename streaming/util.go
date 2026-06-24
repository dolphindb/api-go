package streaming

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/model"
)

func topicSite(topic string) (string, bool) {
	ind := strings.Index(topic, "/")
	if ind <= 0 {
		return "", false
	}

	return topic[:ind], true
}

func getReconnectTimestamp(site string) int64 {
	raw, ok := reconnectTable.Load(site)
	if ok && raw != nil {
		item := raw.(*reconnectItem)
		return item.getTimeStamp()
	}

	return 0
}

func contains(src []string, sub string) bool {
	for _, v := range src {
		if v == sub {
			return true
		}
	}

	return false
}

func addQueue(topic string) (*UnboundedChan, error) {
	if _, ok := queueMap.Load(topic); ok {
		return nil, fmt.Errorf("topic %s already subscribed", topic)
	}

	q := NewUnboundedChan(4096)

	queueMap.Store(topic, q)
	return q, nil
}

func getAllReconnectSites() []string {
	res := make([]string, 0)
	reconnectTable.Range(func(k, v interface{}) bool {
		key := k.(string)
		val := v.(*reconnectItem)
		if val.getState() > 0 {
			res = append(res, key)
		}

		return true
	})

	return res
}

func dispatch(msg IMessage) {
	topicStr := msg.GetTopic()
	topics := strings.Split(topicStr, ",")
	for _, v := range topics {
		topic, ok := haTopicToTrueTopic.Load(v)
		if !ok {
			continue
		}

		sitesRaw, ok := trueTopicToRequests.Load(topic)
		if ok && sitesRaw != nil {
			sites := sitesRaw.([]*SubscribeRequest)
			for _, s := range sites {
				s.Offset += 1
			}
		}

		raw, ok := queueMap.Load(topic)
		if ok && raw != nil {
			q := raw.(*UnboundedChan)
			q.In <- msg
		}
	}
}

func batchDispatch(msg []IMessage) {
	for _, v := range msg {
		addMessageToCache(v)
	}

	flushToQueue()
}

func addMessageToCache(msg IMessage) {
	topicStr := msg.GetTopic()
	topics := strings.Split(topicStr, ",")
	for _, v := range topics {
		topic, ok := haTopicToTrueTopic.Load(v)
		if !ok {
			continue
		}

		cache := make([]IMessage, 0)

		raw, ok := messageCache.Load(topic)
		if ok {
			cache = raw.([]IMessage)
		}

		cache = append(cache, msg)
		messageCache.Store(topic, cache)
	}
}

func flushToQueue() {
	messageCache.Range(func(topic, v interface{}) bool {
		val := v.([]IMessage)

		requests := make([]*SubscribeRequest, 0)
		requestsRaw, ok := trueTopicToRequests.Load(topic)
		if ok && requestsRaw != nil {
			requests = requestsRaw.([]*SubscribeRequest)
		}

		raw, ok := queueMap.Load(topic)
		if ok && raw != nil {
			q := raw.(*UnboundedChan)
			for _, m := range val {
				for _, s := range requests {
					s.Offset += 1
				}
				q.In <- m
			}
		}
		return true
	})

	messageCache = sync.Map{}
}

func getAllTopicBySite(site string) []string {
	res := make([]string, 0)
	trueTopicToRequests.Range(func(k, v interface{}) bool {
		key := k.(string)

		s, ok := topicSite(key)
		if !ok {
			streamingLogWarnf("ignore malformed topic while listing reconnect sites: %q", key)
			return true
		}

		if s == site {
			res = append(res, key)
		}

		return true
	})

	return res
}

func getReconnectItemState(site string) int {
	raw, ok := reconnectTable.Load(site)
	if ok && raw != nil {
		item := raw.(*reconnectItem)
		return item.getState()
	}

	return 0
}

func newConnectedConn(req *SubscribeRequest) (dialer.Conn, error) {
	opt := &dialer.BehaviorOptions{
		EnableScram: req.EnableScram,
	}
	conn, err := dialer.NewConn(context.TODO(), req.Address, opt)
	if err != nil {
		streamingLogErrorf("failed to create a conn: %v", err)
		return nil, err
	}

	conn.SetUserID(req.UserID)
	conn.SetPassword(req.Password)
	err = conn.Connect()
	if err != nil {
		streamingLogErrorf("failed to connect to server: %v", err)
		return nil, err
	}

	return conn, err
}

func newReverseStreamConnectedConn(req *SubscribeRequest) (dialer.Conn, error) {
	opt := &dialer.BehaviorOptions{
		IsReverseStreaming: true,
		EnableScram:        req.EnableScram,
	}

	conn, err := dialer.NewConn(context.TODO(), req.Address, opt)
	if err != nil {
		streamingLogErrorf("failed to create a conn: %v", err)
		return nil, err
	}

	conn.SetUserID(req.UserID)
	conn.SetPassword(req.Password)
	err = conn.Connect()
	if err != nil {
		streamingLogErrorf("failed to connect to server: %v", err)
		return nil, err
	}

	return conn, err
}

func getActiveReq(sites []*SubscribeRequest) *SubscribeRequest {
	ind := 0
	siteNum := len(sites)
	for ind < siteNum {
		si := sites[ind]
		ind = (ind + 1) % siteNum

		conn, err := newConnectedConn(si)
		if err != nil {
			streamingLogErrorf("failed to instantiate a connected conn: %v", err)
			continue
		}

		_, err = conn.RunScript("1")
		if err != nil {
			streamingLogErrorf("failed to call 1: %v", err)
			continue
		}

		conn.Close()

		return si
	}

	return nil
}

func setReconnectTimestamp(site string, v int64) {
	raw, ok := reconnectTable.Load(site)
	if ok && raw != nil {
		s := raw.(*reconnectItem)
		s.setTimeStamp(v)
	}
}

func setReconnectItem(topic string, v int) {
	if topic == "" {
		return
	}

	site, ok := topicSite(topic)
	if !ok {
		streamingLogWarnf("ignore malformed topic while updating reconnect state: %q", topic)
		return
	}

	if raw, ok := reconnectTable.Load(site); ok {
		item := raw.(*reconnectItem)
		item.setState(v).setTimeStamp(time.Now().UnixNano() / 1000000)
	} else {
		item := &reconnectItem{
			reconnectState:         v,
			lastReconnectTimestamp: time.Now().UnixNano() / 1000000,
		}

		item.putTopic(topic)
		reconnectTable.Store(site, item)
	}
}

func getSiteByName(si string) *SubscribeRequest {
	topics := getAllTopicBySite(si)
	if len(topics) > 0 {
		raw, ok := trueTopicToRequests.Load(topics[0])
		if !ok {
			return nil
		}

		sites := raw.([]*SubscribeRequest)
		if len(sites) > 0 {
			return getActiveReq(sites)
		}
	}

	return nil
}

// IsClosed checks whether the topic is closed.
func IsClosed(topic string) bool {
	raw, ok := haTopicToTrueTopic.Load(topic)
	if !ok {
		return true
	}

	topic = raw.(string)

	raw, ok = trueTopicToRequests.Load(topic)
	if !ok {
		return true
	}

	requests := raw.([]*SubscribeRequest)
	if len(requests) == 0 {
		return true
	}

	return requests[0].closed
}

func generatorGetSubscriptionTopicParams(tableName, actionName string) ([]model.DataForm, error) {
	l, err := model.NewDataTypeListFromRawData(model.DtString, []string{tableName, actionName})
	if err != nil {
		streamingLogErrorf("failed to instantiate DataTypeList: %v", err)
		return nil, err
	}

	dfl := make([]model.DataForm, 2)
	dfl[0] = model.NewScalar(l.Get(0))
	dfl[1] = model.NewScalar(l.Get(1))

	return dfl, nil
}

func generatePublishTableParams(s *SubscribeRequest, listenHost string, listenPort int32) ([]model.DataForm, error) {
	pubReq := make([]model.DataForm, 0, 7)
	r, err := packListeningHostAndPort(listenHost, listenPort)
	if err != nil {
		return nil, err
	}

	pubReq = append(pubReq, r...)
	dfl, err := generatorGetSubscriptionTopicParams(s.TableName, s.ActionName)
	if err != nil {
		streamingLogErrorf("failed to generate the params of GetSubscriptionTopic: %v", err)
		return nil, err
	}

	pubReq = append(pubReq, dfl...)
	offset, err := model.NewDataType(model.DtLong, s.Offset)
	if err != nil {
		streamingLogErrorf("failed to instantiate DataType with offset: %v", err)
		return nil, err
	}

	pubReq = append(pubReq, model.NewScalar(offset))
	if s.Filter != nil {
		pubReq = append(pubReq, s.Filter)
	} else {
		void, err := model.NewDataType(model.DtVoid, "")
		if err != nil {
			streamingLogErrorf("failed to instantiate DataType with void: %v", err)
			return nil, err
		}

		pubReq = append(pubReq, model.NewScalar(void))
	}

	if s.AllowExists {
		al, err := model.NewDataType(model.DtBool, true)
		if err != nil {
			streamingLogErrorf("failed to instantiate DataType with AllowExists: %v", err)
			return nil, err
		}
		pubReq = append(pubReq, model.NewScalar(al))
		streamingLogDebugf("publishTable allowExists parameter: %s", pubReq[6].String())
	}

	return pubReq, nil
}

func packListeningHostAndPort(listeningHost string, listeningPort int32) ([]model.DataForm, error) {
	localIP, err := model.NewDataType(model.DtString, listeningHost)
	if err != nil {
		streamingLogErrorf("failed to instantiate DataType with listeningHost: %v", err)
		return nil, err
	}

	port, err := model.NewDataType(model.DtInt, listeningPort)
	if err != nil {
		streamingLogErrorf("failed to instantiate DataType with listeningPort: %v", err)
		return nil, err
	}

	return []model.DataForm{model.NewScalar(localIP), model.NewScalar(port)}, nil
}

func generateStopPublishTableParams(s *SubscribeRequest, listenHost string, listenPort int32) ([]model.DataForm, error) {
	pubReq := make([]model.DataForm, 0)

	localIP, err := model.NewDataType(model.DtString, listenHost)
	if err != nil {
		streamingLogErrorf("failed to instantiate DataType with listeningHost: %v", err)
		return nil, err
	}

	pubReq = append(pubReq, model.NewScalar(localIP))

	port, err := model.NewDataType(model.DtInt, listenPort)
	if err != nil {
		streamingLogErrorf("failed to instantiate DataType with listeningPort: %v", err)
		return nil, err
	}

	pubReq = append(pubReq, model.NewScalar(port))

	dfl, err := generatorGetSubscriptionTopicParams(s.TableName, s.ActionName)
	if err != nil {
		streamingLogErrorf("failed to generate the params of GetSubscriptionTopic: %v", err)
		return nil, err
	}

	pubReq = append(pubReq, dfl...)

	return pubReq, nil
}
