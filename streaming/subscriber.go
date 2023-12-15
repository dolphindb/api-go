package streaming

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dolphindb/api-go/dialer"
	"github.com/dolphindb/api-go/model"
)

type subscriber struct {
	listeningHost string
	listeningPort int32
	once          *sync.Once

	connList *UnboundedChan
}

// SubscribeRequest is used for subscribing.
type SubscribeRequest struct {
	// Server address
	Address string
	// Name of the table to be subscribed
	TableName string
	// Name of the subscription task
	ActionName string
	// If treat data as table
	MsgAsTable bool
	// Offset of subscription
	Offset int64
	// When AllowExists=true, if the topic already exists before subscribing,
	// the server will not throw an exception.
	AllowExists bool
	// The amount of data processed at one time
	BatchSize *int
	// timeout. unit: second
	Throttle *float32
	// whether to allow reconnection
	Reconnect bool

	// Specify parameter Filter with function setStreamTableFilterColumn.
	// SetStreamTableFilterColumn specifies the filtering column of a stream table.
	// Only the messages with filtering column values in filter are subscribed.
	Filter *model.Vector

	// handle subscription information, batchSize must be -1
	Handler MessageHandler
	// batch handle subscription information, batchSize must >= 1
	BatchHandler MessageBatchHandler
	// StreamDeserializer to decode heterogenous streaming
	MsgDeserializer *StreamDeserializer
}

type site struct {
	address     string
	tableName   string
	actionName  string
	msgID       int64
	reconnect   bool
	AllowExists bool
	closed      bool

	filter  *model.Vector
	handler MessageHandler
}

// SetBatchSize sets the batch size.
func (s *SubscribeRequest) SetBatchSize(bs int) *SubscribeRequest {
	s.BatchSize = &bs

	return s
}

// SetThrottleFloat sets the throttle.
func (s *SubscribeRequest) SetThrottle(th float32) *SubscribeRequest {
	s.Throttle = &th

	return s
}

func newSubscriber(subscribeHost string, subscribePort int) *subscriber {
	return &subscriber{
		listeningHost: subscribeHost,
		listeningPort: int32(subscribePort),
		connList:      NewUnboundedChan(1),
		once:          &sync.Once{},
	}
}

func (s *subscriber) subscribeInternal(req *SubscribeRequest) (*UnboundedChan, error) {
	var conn dialer.Conn
	var err error
	if s.listeningPort == 0 {
		conn, err = newReverseStreamConnectedConn(req.Address)
	} else {
		conn, err = newConnectedConn(req.Address)
	}

	if err != nil {
		fmt.Printf("Failed to connect to server: %s\n", err.Error())
		return nil, err
	}

	defer func() {
		if s.listeningPort > 0 {
			conn.Close()
		}
	}()

	topic, err := getTopicFromServer(req.TableName, req.ActionName, conn)
	if err != nil {
		fmt.Printf("Failed to get topic from server: %s\n", err.Error())
		return nil, err
	}

	if s.listeningHost == "" || strings.ToLower(s.listeningHost) == localhost {
		s.listeningHost = conn.GetLocalAddress()
	}

	q, retErr := addQueue(topic)

	err = s.publishTable(topic, req, conn)
	if err != nil {
		if address, ok := getLeader(err.Error()); ok {
			fmt.Println(" loop subscribe internal ")
			req.Address = address
			return s.subscribeInternal(req)
		}
		queueMap.Delete(topic)

		return nil, err
	}

	s.connList.In <- conn

	return q, retErr
}



func (s *subscriber) reSubscribeInternal(req *SubscribeRequest) error {
	var conn dialer.Conn
	var err error
	if s.listeningPort == 0 {
		conn, err = newReverseStreamConnectedConn(req.Address)
	} else {
		conn, err = newConnectedConn(req.Address)
	}

	if err != nil {
		fmt.Printf("Failed to connect to server: %s\n", err.Error())
		return err
	}

	defer func() {
		if s.listeningPort > 0 {
			conn.Close()
		}
	}()

	topic, err := getTopicFromServer(req.TableName, req.ActionName, conn)
	if err != nil {
		fmt.Printf("Failed to get topic from server: %s\n", err.Error())
		return err
	}

	if s.listeningHost == "" || strings.ToLower(s.listeningHost) == localhost {
		s.listeningHost = conn.GetLocalAddress()
	}

	err = s.publishTable(topic, req, conn)
	if err != nil {
		if address, ok := getLeader(err.Error()); ok {
			fmt.Println(" loop resubscribe internal ")
			req.Address = address
			return s.reSubscribeInternal(req)
		}

		return err
	}

	if !s.connList.IsClosed() {
		s.connList.In <- conn
	}

	return nil
}

func getLeader(err string) (string, bool) {
	if !strings.Contains(err, "<NotLeader>") {
		return "", false
	}

	site := strings.Split(err, "<NotLeader>")[1]
	strs := strings.Split(site, ":")
	return fmt.Sprintf("%s:%s", strs[0], strs[1]), true
}

func (s *subscriber) checkServerVersion(address string) error {
	conn, err := newConnectedConn(address)
	if err != nil {
		return err
	}

	defer conn.Close()

	df, err := conn.RunScript("version()")
	if err != nil {
		return err
	}

	ver := df.(*model.Scalar).DataType.String()
	if strings.HasPrefix(ver, "3") || (strings.HasPrefix(ver, "2") && isLater(ver, "2.00.9")) {
		if s.listeningPort != 0 {
			fmt.Println("Warn: The server only supports subscription through reverse connection (connection initiated by the subscriber). The specified port will not take effect.")
		}
		s.listeningPort = 0
	} else if s.listeningPort <= 0 {
		return errors.New("The server does not support subscription through reverse connection (connection initiated by the subscriber). Specify a valid port parameter.")
	}

	return nil
}

func isLater(ori, raw string) bool {
	oris := strings.Split(ori, ".")
	raws := strings.Split(raw, ".")
	for k, v := range oris {
		r := raws[k]
		if len(v) > len(r) || v > r {
			return true
		}
	}

	return false
}

func (s *subscriber) getConn() (net.Conn, bool) {
	select {
		case tc, ok := <-s.connList.Out:
			if ok {
				return tc.(dialer.Conn), true
			} else {
				return nil, false
			}
		default:
			return nil, false
	}
}

func (s *subscriber) publishTable(topic string, req *SubscribeRequest, conn dialer.Conn) error {
	if s.listeningHost == "" || strings.ToLower(s.listeningHost) == localhost {
		s.listeningHost = conn.GetLocalAddress()
	}

	pubReq, err := generatePublishTableParams(req, s.listeningHost, s.listeningPort)
	if err != nil {
		fmt.Printf("Failed to generate the params of PublishTable: %s\n", err.Error())
		return err
	}
	df, err := conn.RunFunc("publishTable", pubReq)
	if err != nil {
		fmt.Printf("Failed to publish table: %s\n", err.Error())
		return err
	}

	if df.GetDataForm() == model.DfVector && df.GetDataType() == model.DtAny {
		err = s.handleAnyVector(topic, df, req)
		if err != nil {
			fmt.Printf("Failed to handle vector: %s\n", err.Error())
			return err
		}
	} else {
		s.packSite(topic, req)
	}

	return nil
}

func (s *subscriber) packSite(topic string, req *SubscribeRequest) {
	si := &site{
		address:     req.Address,
		tableName:   req.TableName,
		actionName:  req.ActionName,
		handler:     req.Handler,
		msgID:       req.Offset - 1,
		reconnect:   req.Reconnect,
		filter:      req.Filter,
		AllowExists: req.AllowExists,
	}

	haTopicToTrueTopic.Store(topic, topic)
	trueTopicToSites.Store(topic, []*site{si})
}

func (s *subscriber) getTopicFromServer(address, tableName, actionName string) (string, error) {
	conn, err := newConnectedConn(address)
	if err != nil {
		fmt.Printf("Failed to connect to server: %s\n", err.Error())
		return "", err
	}

	defer conn.Close()

	return getTopicFromServer(tableName, actionName, conn)
}

func getTopicFromServer(tableName, actionName string, conn dialer.Conn) (string, error) {
	params, err := generatorGetSubscriptionTopicParams(tableName, actionName)
	if err != nil {
		fmt.Printf("Failed to generate the params of GetSubscriptionTopic: %s\n", err.Error())
		return "", err
	}

	df, err := conn.RunFunc("getSubscriptionTopic", params)
	if err != nil {
		fmt.Printf("Failed to call getSubscriptionTopic: %s\n", err.Error())
		return "", err
	}

	vct := df.(*model.Vector)
	sca := vct.Data.ElementValue(0).(*model.Scalar)
	return sca.DataType.String(), nil
}

func (s *subscriber) handleAnyVector(topic string, df model.DataForm, req *SubscribeRequest) error {
	vct := df.(*model.Vector)
	v := vct.Data.ElementValue(1).(*model.Vector)
	HASiteStrings := v.Data.StringList()
	sites := make([]*site, len(HASiteStrings))
	for k, v := range HASiteStrings {
		str := strings.Split(v, ":")
		host := str[0]
		port, err := strconv.Atoi(str[1])
		if err != nil {
			fmt.Printf("Failed to parse server port: %s\n", err.Error())
			return err
		}

		alias := str[2]

		sites[k] = &site{
			address:     fmt.Sprintf("%s:%d", host, port),
			tableName:   req.TableName,
			actionName:  req.ActionName,
			msgID:       req.Offset - 1,
			handler:     req.Handler,
			reconnect:   true,
			filter:      req.Filter,
			AllowExists: req.AllowExists,
		}

		haTopicToTrueTopic.Store(fmt.Sprintf("%s:%d:%s/%s/%s", host, port, alias, req.TableName, req.ActionName), topic)
	}

	trueTopicToSites.Store(topic, sites)

	return nil
}

func (s *subscriber) activeCloseConnection(si *site) error {
	conn, err := newConnectedConn(si.address)
	if err != nil {
		fmt.Printf("Failed to new a connected connection: %s\n", err.Error())
		return err
	}

	defer conn.Close()

	verNum, err := s.getVersion(conn)
	if err != nil {
		return err
	}

	err = s.activeClosePublishConnection(verNum, conn)
	if err != nil {
		fmt.Printf("Failed to call activeClosePublishConnection: %s\n", err.Error())
		return err
	}

	time.Sleep(1 * time.Second)
	return nil
}

func (s *subscriber) activeClosePublishConnection(verNum int, conn dialer.Conn) error {
	if s.listeningHost == "" || strings.ToLower(s.listeningHost) == localhost {
		s.listeningHost = conn.GetLocalAddress()
	}

	params, err := s.packActiveClosePublishConnectionParams(verNum)
	if err != nil {
		fmt.Printf("Failed to pack params: %s\n", err.Error())
		return err
	}

	_, err = conn.RunFunc("activeClosePublishConnection", params)
	if err != nil {
		fmt.Printf("Failed to call activeClosePublishConnection: %s\n", err.Error())
		return err
	}

	return nil
}

func (s *subscriber) getVersion(conn dialer.Conn) (int, error) {
	df, err := conn.RunScript("version()")
	if err != nil {
		fmt.Printf("Failed to call vesion(): %s\n", err.Error())
		return 0, err
	}

	sca := df.(*model.Scalar)
	verStr := sca.DataType.String()
	return getVersionNum(verStr), nil
}

func (s *subscriber) packActiveClosePublishConnectionParams(verNum int) ([]model.DataForm, error) {
	params := make([]model.DataForm, 3)

	localIP, err := model.NewDataType(model.DtString, s.listeningHost)
	if err != nil {
		fmt.Printf("Failed to instantiate DataType with listeningHost: %s\n", err.Error())
		return nil, err
	}

	params[0] = model.NewScalar(localIP)

	port, err := model.NewDataType(model.DtInt, s.listeningPort)
	if err != nil {
		fmt.Printf("Failed to instantiate DataType with listeningPort: %s\n", err.Error())
		return nil, err
	}

	params[1] = model.NewScalar(port)
	if verNum >= 955 {
		tmp, err := model.NewDataType(model.DtBool, byte(1))
		if err != nil {
			fmt.Printf("Failed to instantiate DataType with bool value: %s\n", err.Error())
			return nil, err
		}

		params[2] = model.NewScalar(tmp)
	}

	return params, nil
}

func (s *subscriber) unSubscribe(req *SubscribeRequest) error {
	conn, err := newConnectedConn(req.Address)
	if err != nil {
		fmt.Printf("Failed to new connected conn: %s\n", err.Error())
		return err
	}

	defer conn.Close()

	topic, err := getTopicFromServer(req.TableName, req.ActionName, conn)
	if err != nil {
		fmt.Printf("Failed to get topic from server: %s\n", err.Error())
		return nil
	}

	fmt.Println("Successfully unsubscribe from the table ", topic)

	s.cleanTopic(topic)
	err = s.stopPublishTable(req, conn)
	if err != nil {
		return err
	}

	return nil
}

func (s *subscriber) cleanTopic(topic string) {
	// queueMap.Delete(topic)

	raw, ok := trueTopicToSites.Load(topic)
	if !ok {
		return
	}

	sites := raw.([]*site)

	for _, v := range sites {
		v.closed = true
	}
}

func (s *subscriber) stopPublishTable(req *SubscribeRequest, conn dialer.Conn) error {
	if s.listeningHost == "" || strings.ToLower(s.listeningHost) == localhost {
		s.listeningHost = conn.GetLocalAddress()
	}

	stopReq, err := generateStopPublishTableParams(req, s.listeningHost, s.listeningPort)
	if err != nil {
		fmt.Printf("Failed to generate the params of stopPublishTable: %s\n", err.Error())
		return err
	}

	_, err = conn.RunFunc("stopPublishTable", stopReq)
	if err != nil {
		fmt.Printf("Failed to call stopPublishTable: %s\n", err.Error())
		return err
	}

	return nil
}

func tryReconnect(topic string, ac AbstractClient) {
	topicRaw, ok := haTopicToTrueTopic.Load(topic)
	if !ok {
		return
	}

	// queueMap.Delete(topicRaw)

	sites, isSuccess := loadSites(topicRaw)
	if !isSuccess {
		return
	}

	site := getActiveSite(sites)
	if site != nil {
		if ac.doReconnect(site) {
			reconnectTable.Delete(site)
			waitReconnectTopic.Delete(topicRaw)
			return
		}

		waitReconnectTopic.Store(topicRaw, topicRaw)
	}
}

func loadSites(topic interface{}) ([]*site, bool) {
	raw, ok := trueTopicToSites.Load(topic)
	if !ok {
		return nil, false
	}

	sites := raw.([]*site)

	if len(sites) == 0 || (len(sites) == 1 && !sites[0].reconnect) {
		return nil, false
	}

	return sites, true
}
