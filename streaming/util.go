package streaming

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dolphindb/api-go/dialer"
	"github.com/dolphindb/api-go/model"

	"github.com/smallnest/chanx"
)

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

func addQueue(topic string) (*chanx.UnboundedChan, error) {
	if _, ok := queueMap.Load(topic); ok {
		return nil, fmt.Errorf("topic %s already subscribed", topic)
	}

	q := chanx.NewUnboundedChan(4096)
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

		raw, ok := queueMap.Load(topic)
		if ok && raw != nil {
			q := raw.(*chanx.UnboundedChan)
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
	messageCache.Range(func(k, v interface{}) bool {
		val := v.([]IMessage)

		raw, ok := queueMap.Load(k)
		if ok && raw != nil {
			q := raw.(*chanx.UnboundedChan)
			for _, m := range val {
				q.In <- m
			}
		}
		return true
	})

	messageCache = sync.Map{}
}

func getAllTopicBySite(site string) []string {
	res := make([]string, 0)
	trueTopicToSites.Range(func(k, v interface{}) bool {
		key := k.(string)

		s := key[0:strings.Index(key, "/")]
		if s == site {
			res = append(res, key)
		}

		return true
	})

	return res
}

func getNeedReconnect(site string) int {
	raw, ok := reconnectTable.Load(site)
	if ok && raw != nil {
		item := raw.(*reconnectItem)
		return item.getState()
	}

	return 0
}

func newConnectedConn(address string) (dialer.Conn, error) {
	conn, err := dialer.NewConn(context.TODO(), address, nil)
	if err != nil {
		fmt.Printf("Failed to new a conn: %s\n", err.Error())
		return nil, err
	}

	err = conn.Connect()
	if err != nil {
		fmt.Printf("Failed to connect to server: %s\n", err.Error())
		return nil, err
	}

	return conn, err
}

func getActiveSite(sites []*site) *site {
	ind := 0
	siteNum := len(sites)
	for ind < siteNum {
		si := sites[ind]
		ind = (ind + 1) % siteNum

		conn, err := newConnectedConn(si.address)
		if err != nil {
			fmt.Printf("Failed to instantiate a connected conn: %s\n", err.Error())
			continue
		}

		_, err = conn.RunScript("1")
		if err != nil {
			fmt.Printf("Failed to call 1: %s\n", err.Error())
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

func setNeedReconnect(topic string, v int) {
	if topic == "" {
		return
	}

	site := topic[0:strings.Index(topic, "/")]
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

func getSiteByName(si string) *site {
	topics := getAllTopicBySite(si)
	if len(topics) > 0 {
		raw, ok := trueTopicToSites.Load(topics[0])
		if !ok {
			return nil
		}

		sites := raw.([]*site)
		if len(sites) > 0 {
			return getActiveSite(sites)
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

	raw, ok = trueTopicToSites.Load(topic)
	if !ok {
		return true
	}

	sites := raw.([]*site)
	if len(sites) == 0 {
		return true
	}

	return sites[0].closed
}

func generatorGetSubscriptionTopicParams(tableName, actionName string) ([]model.DataForm, error) {
	l, err := model.NewDataTypeListWithRaw(model.DtString, []string{tableName, actionName})
	if err != nil {
		fmt.Printf("Failed to instantiate DataTypeList: %s\n", err.Error())
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
		fmt.Printf("Failed to generate the params of GetSubscriptionTopic:%s\n", err.Error())
		return nil, err
	}

	pubReq = append(pubReq, dfl...)
	offset, err := model.NewDataType(model.DtLong, s.Offset)
	if err != nil {
		fmt.Printf("Failed to instantiate DataType with offset: %s\n", err.Error())
		return nil, err
	}

	pubReq = append(pubReq, model.NewScalar(offset))
	if s.Filter != nil {
		pubReq = append(pubReq, s.Filter)
	} else {
		void, err := model.NewDataType(model.DtVoid, "")
		if err != nil {
			fmt.Printf("Failed to instantiate DataType with void: %s\n", err.Error())
			return nil, err
		}

		pubReq = append(pubReq, model.NewScalar(void))
	}

	if s.AllowExists {
		al, err := model.NewDataType(model.DtBool, byte(1))
		if err != nil {
			fmt.Printf("Failed to instantiate DataType with AllowExists: %s\n", err.Error())
			return nil, err
		}

		pubReq = append(pubReq, model.NewScalar(al))
		return pubReq, nil
	}

	return pubReq[:6], nil
}

func packListeningHostAndPort(listeningHost string, listeningPort int32) ([]model.DataForm, error) {
	localIP, err := model.NewDataType(model.DtString, listeningHost)
	if err != nil {
		fmt.Printf("Failed to instantiate DataType with listeningHost: %s\n", err.Error())
		return nil, err
	}

	port, err := model.NewDataType(model.DtInt, listeningPort)
	if err != nil {
		fmt.Printf("Failed to instantiate DataType with listeningPort: %s\n", err.Error())
		return nil, err
	}

	return []model.DataForm{model.NewScalar(localIP), model.NewScalar(port)}, nil
}

func generateStopPublishTableParams(s *SubscribeRequest, listenHost string, listenPort int32) ([]model.DataForm, error) {
	pubReq := make([]model.DataForm, 0)

	localIP, err := model.NewDataType(model.DtString, listenHost)
	if err != nil {
		fmt.Printf("Failed to instantiate DataType with listeningHost: %s\n", err.Error())
		return nil, err
	}

	pubReq = append(pubReq, model.NewScalar(localIP))

	port, err := model.NewDataType(model.DtInt, listenPort)
	if err != nil {
		fmt.Printf("Failed to instantiate DataType with listeningPort: %s\n", err.Error())
		return nil, err
	}

	pubReq = append(pubReq, model.NewScalar(port))

	dfl, err := generatorGetSubscriptionTopicParams(s.TableName, s.ActionName)
	if err != nil {
		fmt.Printf("Failed to generate the params of GetSubscriptionTopic: %s\n", err.Error())
		return nil, err
	}

	pubReq = append(pubReq, dfl...)

	return pubReq, nil
}

func getVersionNum(ver string) int {
	if str := strings.Split(ver, " "); len(str) >= 2 {
		verStr := strings.ReplaceAll(str[0], ".", "")
		verNum, err := strconv.Atoi(verStr)
		if err == nil {
			return verNum
		}
	}

	return 0
}
