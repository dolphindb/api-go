package streaming

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/model"
)

type subscriber struct {
	listeningHost string
	listeningPort int32
	once          *sync.Once
	version       string

	connList *UnboundedChan
}

type streamFailoverAction struct {
	address         string
	friendlyMessage string
}

// SubscribeRequest is used for subscribing.
type SubscribeRequest struct {
	// Server address
	Address string
	// the user ID
	UserID string
	// password of the user
	Password string
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

	// Whether the subscription is closed
	closed bool

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
	// if enable SCRAM verify
	EnableScram bool
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
		conn, err = newReverseStreamConnectedConn(req)
	} else {
		conn, err = newConnectedConn(req)
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
		if action, ok := getStreamFailoverAction(err, req.Address); ok {
			if action.friendlyMessage != "" {
				fmt.Println(action.friendlyMessage)
			}
			if action.address != "" {
				req.Address = action.address
				return s.subscribeInternal(req)
			}
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
		conn, err = newReverseStreamConnectedConn(req)
	} else {
		conn, err = newConnectedConn(req)
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
		if action, ok := getStreamFailoverAction(err, req.Address); ok {
			if action.friendlyMessage != "" {
				fmt.Println(action.friendlyMessage)
			}
			if action.address != "" {
				req.Address = action.address
				return s.reSubscribeInternal(req)
			}
			if nextReq := s.nextFailoverRequest(topic, req.Address); nextReq != nil {
				return s.reSubscribeInternal(nextReq)
			}
		}

		return err
	}

	if !s.connList.IsClosed() {
		s.connList.In <- conn
	}

	return nil
}

func getStreamFailoverAction(err error, currentAddress string) (streamFailoverAction, bool) {
	serverErr, ok := dialer.AsServerError(err)
	if !ok {
		return streamFailoverAction{}, false
	}

	switch serverErr.Code {
	case dialer.ServerErrNotLeader:
		nextAddress, ok := serverErr.TargetAddress()
		if !ok {
			return streamFailoverAction{}, false
		}
		return streamFailoverAction{
			address:         nextAddress,
			friendlyMessage: fmt.Sprintf("Streaming subscription is unavailable on node %s because this node is not the current raft leader. The client will switch to %s.", currentAddress, nextAddress),
		}, true
	case dialer.ServerErrUnknownLeader:
		return streamFailoverAction{
			friendlyMessage: fmt.Sprintf("Streaming subscription is temporarily unavailable on node %s because it cannot determine the current raft leader. The client will try high-availability failover.", currentAddress),
		}, true
	default:
		return streamFailoverAction{}, false
	}
}

func (s *subscriber) nextFailoverRequest(topic, currentAddress string) *SubscribeRequest {
	raw, ok := trueTopicToRequests.Load(topic)
	if !ok || raw == nil {
		return nil
	}

	requests := raw.([]*SubscribeRequest)
	for _, candidate := range requests {
		if candidate != nil && candidate.Address != currentAddress {
			return candidate
		}
	}

	return nil
}

func (s *subscriber) isReverseStreaming() bool {
	return strings.HasPrefix(s.version, "3") || (strings.HasPrefix(s.version, "2") && isLater(s.version, "2.00.9"))
}

func (s *subscriber) checkServerVersion(req *SubscribeRequest) error {
	conn, err := newConnectedConn(req)
	if err != nil {
		return err
	}

	defer conn.Close()

	df, err := conn.RunScript("version()")
	if err != nil {
		return err
	}

	s.version = df.(*model.Scalar).DataType.String()
	if s.isReverseStreaming() {
		if s.listeningPort != 0 {
			fmt.Println("Warn: The server only supports subscription through reverse connection (connection initiated by the subscriber). The specified port will not take effect.")
		}
		s.listeningPort = 0
	} else if s.listeningPort <= 0 {
		return errors.New("the server does not support subscription through reverse connection (connection initiated by the subscriber). Specify a valid port parameter")
	}

	return nil
}

func isLater(ori, raw string) bool {
	oris := strings.Split(strings.Split(ori, " ")[0], ".") // remove content after whitespace
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
		s.packRequest(topic, req)
	}

	return nil
}

func (s *subscriber) packRequest(topic string, req *SubscribeRequest) {
	newReq := &SubscribeRequest{
		Address:     req.Address,
		UserID:      req.UserID,
		Password:    req.Password,
		TableName:   req.TableName,
		ActionName:  req.ActionName,
		Handler:     req.Handler,
		Offset:      req.Offset - 1,
		Reconnect:   req.Reconnect,
		Filter:      req.Filter,
		AllowExists: req.AllowExists,
	}

	haTopicToTrueTopic.Store(topic, topic)
	trueTopicToRequests.Store(topic, []*SubscribeRequest{newReq})
}

func (s *subscriber) getTopicFromServer(req *SubscribeRequest) (string, error) {
	conn, err := newConnectedConn(req)
	if err != nil {
		fmt.Printf("Failed to connect to server: %s\n", err.Error())
		return "", err
	}

	defer conn.Close()

	return getTopicFromServer(req.TableName, req.ActionName, conn)
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
	requests := make([]*SubscribeRequest, len(HASiteStrings))
	for k, v := range HASiteStrings {
		str := strings.Split(v, ":")
		host := str[0]
		port, err := strconv.Atoi(str[1])
		if err != nil {
			fmt.Printf("Failed to parse server port: %s\n", err.Error())
			return err
		}

		alias := str[2]

		requests[k] = &SubscribeRequest{
			Address:     fmt.Sprintf("%s:%d", host, port),
			UserID:      req.UserID,
			Password:    req.Password,
			TableName:   req.TableName,
			ActionName:  req.ActionName,
			Offset:      req.Offset - 1,
			Handler:     req.Handler,
			Reconnect:   true,
			Filter:      req.Filter,
			AllowExists: req.AllowExists,
		}

		haTopicToTrueTopic.Store(fmt.Sprintf("%s:%d:%s/%s/%s", host, port, alias, req.TableName, req.ActionName), topic)
	}

	trueTopicToRequests.Store(topic, requests)

	return nil
}

func (s *subscriber) activeCloseConnection(req *SubscribeRequest) error {
	conn, err := newConnectedConn(req)
	if err != nil {
		fmt.Printf("Failed to new a connected connection: %s\n", err.Error())
		return err
	}

	defer conn.Close()

	err = s.activeClosePublishConnection(conn, req.ActionName, req.TableName)
	if err != nil {
		fmt.Printf("Failed to call activeClosePublishConnection: %s\n", err.Error())
		return err
	}

	time.Sleep(1 * time.Second)
	return nil
}

func (s *subscriber) activeClosePublishConnection(conn dialer.Conn, actionName, tableName string) error {
	if s.listeningHost == "" || strings.ToLower(s.listeningHost) == localhost {
		s.listeningHost = conn.GetLocalAddress()
	}

	params, err := s.packActiveClosePublishConnectionParams(actionName, tableName)
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

func (s *subscriber) packActiveClosePublishConnectionParams(actionName, tableName string) ([]model.DataForm, error) {
	if s.isReverseStreaming() {
		params := make([]model.DataForm, 2)

		actionNameArgs, err := model.NewDataType(model.DtString, actionName)
		if err != nil {
			fmt.Printf("Failed to instantiate DataType with listeningHost: %s\n", err.Error())
			return nil, err
		}

		params[0] = model.NewScalar(actionNameArgs)

		tableNameArgs, err := model.NewDataType(model.DtString, tableName)
		if err != nil {
			fmt.Printf("Failed to instantiate DataType with listeningPort: %s\n", err.Error())
			return nil, err
		}

		params[1] = model.NewScalar(tableNameArgs)
		return params, nil
	} else {
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
		tmp, err := model.NewDataType(model.DtBool, byte(1))
		if err != nil {
			fmt.Printf("Failed to instantiate DataType with bool value: %s\n", err.Error())
			return nil, err
		}

		params[2] = model.NewScalar(tmp)
		return params, nil
	}
}

func (s *subscriber) unSubscribe(req *SubscribeRequest) error {
	conn, err := newConnectedConn(req)
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

	raw, ok := trueTopicToRequests.Load(topic)
	if !ok {
		return
	}

	requests := raw.([]*SubscribeRequest)

	for _, v := range requests {
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

	requests, isSuccess := loadRequests(topicRaw)
	if !isSuccess {
		return
	}

	req := getActiveReq(requests)
	if req != nil {
		if ac.doReconnect(req) {
			reconnectTable.Delete(req)
			waitReconnectTopic.Delete(topicRaw)
			return
		}

		waitReconnectTopic.Store(topicRaw, topicRaw)
	}
}

func loadRequests(topic interface{}) ([]*SubscribeRequest, bool) {
	raw, ok := trueTopicToRequests.Load(topic)
	if !ok {
		return nil, false
	}

	requests := raw.([]*SubscribeRequest)

	if len(requests) == 0 || (len(requests) == 1 && !requests[0].Reconnect) {
		return nil, false
	}

	return requests, true
}
