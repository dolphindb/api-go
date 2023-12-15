package streaming

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/example/util"
	"github.com/dolphindb/api-go/model"
	"github.com/stretchr/testify/assert"
)

const testAddr = "127.0.0.1:3000"

var subscribeServer = make([]net.Conn, 0)

var (
	successResponse = []byte{0x32, 0x30, 0x32, 0x36, 0x37, 0x33, 0x35, 0x39, 0x20, 0x30, 0x20, 0x31, 0x0a, 0x4f, 0x4b, 0x0a}
	versionResponse = []byte{0x32, 0x30, 0x32, 0x36, 0x37, 0x33, 0x35, 0x39, 0x20, 0x31, 0x20, 0x31, 0x0a, 0x4f, 0x4b, 0x0a,
		0x12, 0x00, 0x31, 0x20, 0x63, 0x00}

	pollingGetSubscriptionTopicResponse = []byte{0x32, 0x30, 0x32, 0x36, 0x37, 0x33, 0x35, 0x39, 0x20, 0x31, 0x20, 0x31, 0x0a, 0x4f, 0x4b, 0x0a,
		0x19, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x00, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31,
		0x3a, 0x33, 0x30, 0x30, 0x30, 0x3a, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x33, 0x30, 0x30, 0x30, 0x2f, 0x70, 0x6f, 0x6c, 0x6c, 0x69,
		0x6e, 0x67, 0x2f, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x31, 0x00}
	pollingGetSubscriptionTopicFailedResponse = []byte{0x32, 0x30, 0x32, 0x36, 0x37, 0x33, 0x35, 0x39, 0x20, 0x31, 0x20, 0x31, 0x0a, 0x40, 0x4b, 0x0a}
	pollingPublishTableResponse               = []byte{0x32, 0x30, 0x32, 0x36, 0x37, 0x33, 0x35, 0x39, 0x20, 0x31, 0x20, 0x31, 0x0a, 0x4f, 0x4b, 0x0a,
		0x19, 0x01, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
		0x00, 0x12, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31,
		0x3a, 0x33, 0x30, 0x30, 0x30, 0x3a, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x33, 0x30, 0x30, 0x30, 0x00}

	clientGetSubscriptionTopicResponse = []byte{0x32, 0x30, 0x32, 0x36, 0x37, 0x33, 0x35, 0x39, 0x20, 0x31, 0x20, 0x31, 0x0a, 0x4f, 0x4b, 0x0a,
		0x19, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x00, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31,
		0x3a, 0x33, 0x30, 0x30, 0x30, 0x3a, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x33, 0x30, 0x30, 0x30, 0x2f, 0x74, 0x68, 0x72, 0x65, 0x61,
		0x64, 0x65, 0x64, 0x2f, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x31, 0x00}
	clientGetSubscriptionTopicFailedResponse = []byte{0x32, 0x30, 0x32, 0x36, 0x37, 0x33, 0x35, 0x39, 0x20, 0x31, 0x20, 0x31, 0x0a, 0x40, 0x4b, 0x0a}
	clientgPublishTableResponse              = []byte{0x32, 0x30, 0x32, 0x36, 0x37, 0x33, 0x35, 0x39, 0x20, 0x31, 0x20, 0x31, 0x0a, 0x4f, 0x4b, 0x0a,
		0x12, 0x00, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x33, 0x30, 0x30, 0x30, 0x00}

	pooledGetSubscriptionTopicResponse = []byte{0x32, 0x30, 0x32, 0x36, 0x37, 0x33, 0x35, 0x39, 0x20, 0x31, 0x20, 0x31, 0x0a, 0x4f, 0x4b, 0x0a,
		0x19, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x00, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31,
		0x3a, 0x33, 0x30, 0x30, 0x30, 0x3a, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x33, 0x30, 0x30, 0x30, 0x2f, 0x70, 0x6f, 0x6f, 0x6c, 0x65,
		0x64, 0x2f, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x31, 0x00}
	pooledGetSubscriptionTopicFailedResponse = []byte{0x32, 0x30, 0x32, 0x36, 0x37, 0x33, 0x35, 0x39, 0x20, 0x31, 0x20, 0x31, 0x0a, 0x40, 0x4b, 0x0a}
	pooledgPublishTableResponse              = []byte{0x32, 0x30, 0x32, 0x36, 0x37, 0x33, 0x35, 0x39, 0x20, 0x31, 0x20, 0x31, 0x0a, 0x4f, 0x4b, 0x0a,
		0x19, 0x01, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
		0x00, 0x12, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31,
		0x3a, 0x33, 0x30, 0x30, 0x30, 0x3a, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x33, 0x30, 0x30, 0x30, 0x00}
)

func TestPollingClient(t *testing.T) {
	pc := NewPollingClient(localhost, 3111)

	req := &SubscribeRequest{
		Address:    testAddr,
		TableName:  "polling",
		ActionName: "action1",
		Offset:     0,
		Reconnect:  true,
	}

	poller, err := pc.Subscribe(req)
	assert.Nil(t, err)

	msg := poller.Poll(10, 10)
	assert.Equal(t, len(msg), 0)

	sendOneSubscription(DefaultPort)

	sub := pc.getSubscriber()
	assert.Equal(t, sub.listeningPort, int32(3111))
	assert.Equal(t, strings.HasPrefix(sub.listeningHost, "127.0.0.1"), true)

	item := &reconnectItem{
		reconnectState:         1,
		lastReconnectTimestamp: time.Now().UnixNano() / 1000000,
	}

	item.putTopic("127.0.0.1:3000:local3000/polling/action1")
	reconnectTable.Store("127.0.0.1:3000:local3000", item)

	time.Sleep(4 * time.Second)

	err = pc.UnSubscribe(req)
	assert.Nil(t, err)

	req.ActionName = failedAction
	_, err = pc.Subscribe(req)
	assert.Equal(t, err.Error(), "client error response. @K")

	pc.Close()
}

func TestMain(m *testing.M) {
	exit := make(chan bool)
	ln, err := net.Listen("tcp", testAddr)
	if err != nil {
		return
	}

	go func() {
		for !isExit(exit) {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			go handleTestData(conn)
		}

		ln.Close()
	}()

	exitCode := m.Run()

	close(exit)

	reconnectTable.Delete("127.0.0.1:3000:local3000")
	queueMap.Delete("topic")
	haTopicToTrueTopic.Delete("topic")
	messageCache.Delete("topic")
	trueTopicToSites.Delete("topic")

	for _, v := range subscribeServer {
		v.Close()
	}

	os.Exit(exitCode)
}

func sendOneSubscription(port int) {
	d := net.Dialer{}

	dc, err := d.DialContext(context.TODO(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return
	}

	_, err = dc.Write([]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31, 0x3a, 0x33, 0x30, 0x30, 0x30, 0x3a,
		0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x33, 0x30, 0x30, 0x30, 0x00, 0x19, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x12, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x70, 0x6e, 0x6f, 0x00})
	if err != nil {
		return
	}
	subscribeServer = append(subscribeServer, dc)
}

func sendMoreSubscription(port int) {
	d := net.Dialer{}

	dc, err := d.DialContext(context.TODO(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return
	}

	_, _ = dc.Write([]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31, 0x3a, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x33, 0x30, 0x30, 0x30, 0x2f, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
		0x00, 0x00, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x00, 0x63, 0x6f, 0x6c, 0x00, 0x12, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00,
		0x00, 0x00, 0x63, 0x6f, 0x6c, 0x31, 0x00})

	_, _ = dc.Write([]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31,
		0x3a, 0x33, 0x30, 0x30, 0x30, 0x3a, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x33, 0x30, 0x30, 0x30, 0x00, 0x19,
		0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x01, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x70,
		0x6e, 0x6f, 0x00, 0x70, 0x6f, 0x6e, 0x00})

	subscribeServer = append(subscribeServer, dc)
}

func handleTestData(conn net.Conn) {
	res := make([]byte, 0)
	for {
		buf := make([]byte, 512)
		l, err := conn.Read(buf)
		if err != nil {
			continue
		}

		res = append(res, buf[0:l]...)
		tmp := string(res)
		length := len(res)
		var resp []byte
		switch {
		case length == 85 && strings.Contains(tmp, "getSubscriptionTopic"):
			resp = pollingGetSubscriptionTopicFailedResponse
		case length == 86 && strings.Contains(tmp, "getSubscriptionTopic"):
			resp = clientGetSubscriptionTopicFailedResponse
		case length == 34 && strings.Contains(tmp, "version"):
			resp = versionResponse
		case isSuccessRequest(length):
			resp = successResponse
		case length == 80 && strings.Contains(tmp, "getSubscriptionTopic") && strings.Contains(tmp, "polling"):
			resp = pollingGetSubscriptionTopicResponse
		case length == 103 && strings.Contains(tmp, "publishTable") && strings.Contains(tmp, "polling"):
			resp = pollingPublishTableResponse
		case length == 81 && strings.Contains(tmp, "getSubscriptionTopic"):
			resp = clientGetSubscriptionTopicResponse
		case length == 104 && strings.Contains(tmp, "publishTable"):
			resp = clientgPublishTableResponse
		case length == 79 && strings.Contains(tmp, "getSubscriptionTopic") && strings.Contains(tmp, "pooled"):
			resp = pooledGetSubscriptionTopicResponse
		case length == 84 && strings.Contains(tmp, "getSubscriptionTopic"):
			resp = pooledGetSubscriptionTopicFailedResponse
		case length == 102 && strings.Contains(tmp, "publishTable"):
			resp = pooledgPublishTableResponse
		}

		if resp != nil {
			_, err = conn.Write(resp)
			if err != nil {
				return
			}

			res = make([]byte, 0)
		}
	}
}

var successLength = []int{49, 15, 25, 254, 86, 94, 87, 95, 85, 93}

func isSuccessRequest(l int) bool {
	for _, v := range successLength {
		if v == l {
			return true
		}
	}

	return false
}

func isExit(exit <-chan bool) bool {
	select {
	case <-exit:
		return true
	default:
		return false
	}
}




func TestPollingClientNormal(t *testing.T) {
	host := "localhost:8848";
	db, err := api.NewDolphinDBClient(context.TODO(), host, nil)

	util.AssertNil(err)
    loginReq := &api.LoginRequest{
        UserID:   "admin",
        Password: "123456",
    }

	err = db.Connect()
	util.AssertNil(err)

    err = db.Login(loginReq)
	util.AssertNil(err)

	_,err = db.RunScript(scripts)
	util.AssertNil(err)

	client := NewPollingClient("localhost", 8848)

	req := &SubscribeRequest{
		Address:    "localhost:8848",
		TableName:  "outTables",
		ActionName: "action1",
		MsgAsTable: false,
		Offset:     0,
		Reconnect:  true,
	}

	poller, err := client.Subscribe(req)
	util.AssertNil(err)

	time.Sleep(time.Duration(1)*time.Second)
	msgVec := poller.Poll(1, 6)
	assert.Equal(t, 6, len(msgVec))
	for _, v := range msgVec {
		assert.Equal(t, model.DtBlob, v.GetValueByName("blob").GetDataType())
		assert.Equal(t, model.DtSymbol, v.GetValueByName("sym").GetDataType())
		assert.Equal(t, model.DtTimestamp, v.GetValueByName("timestampv").GetDataType())
	}
}

func TestPollingClientMsgAsTable(t *testing.T) {
	host := "localhost:8848";
	db, err := api.NewDolphinDBClient(context.TODO(), host, nil)

	util.AssertNil(err)
    loginReq := &api.LoginRequest{
        UserID:   "admin",
        Password: "123456",
    }

	err = db.Connect()
	util.AssertNil(err)

    err = db.Login(loginReq)
	util.AssertNil(err)

	_,err = db.RunScript(scripts)
	util.AssertNil(err)

	client := NewPollingClient("localhost", 8848)

	req := &SubscribeRequest{
		Address:    "localhost:8848",
		TableName:  "outTables",
		ActionName: "action1",
		MsgAsTable: true,
		Offset:     0,
		Reconnect:  true,
	}

	poller, err := client.Subscribe(req)
	util.AssertNil(err)

	time.Sleep(time.Duration(1)*time.Second)
	msgVec := poller.Poll(1, 6)
	tbl := msgVec[0]
	assert.Equal(t, 6, tbl.Size())
	symVec := tbl.GetValueByName("sym").(*model.Vector).GetRawValue()
	for _,v := range symVec {
		assert.True(t, v=="msg1" || v=="msg2")
	}
	assert.Equal(t, model.DfVector, tbl.GetValueByName("blob").GetDataForm())
}
func TestPollingClientStreamDeserializer(t *testing.T) {
	host := "localhost:8848";
	db, err := api.NewDolphinDBClient(context.TODO(), host, nil)

	util.AssertNil(err)
    loginReq := &api.LoginRequest{
        UserID:   "admin",
        Password: "123456",
    }

	err = db.Connect()
	util.AssertNil(err)

    err = db.Login(loginReq)
	util.AssertNil(err)

	_,err = db.RunScript(scripts)
	util.AssertNil(err)

	client := NewPollingClient("localhost", 8848)

	sdMap := make(map[string][2]string)
	sdMap["msg1"] = [2]string{"", "pt1"}
	sdMap["msg2"] = [2]string{"", "pt2"}

	opt := StreamDeserializerOption {
		TableNames: sdMap,
		Conn:       db,
	}
	sd, err := NewStreamDeserializer(&opt)
	util.AssertNil(err)

	req := &SubscribeRequest{
		Address:    "localhost:8848",
		TableName:  "outTables",
		ActionName: "action1",
		Offset:     0,
		Reconnect:  true,
		MsgDeserializer:  sd,
	}

	poller, err := client.Subscribe(req)
	util.AssertNil(err)

	time.Sleep(time.Duration(1)*time.Second)
	msgs := poller.Poll(1, 6)
	assert.Equal(t, len(msgs), 6)
	msg1StrVec := make([]interface{}, 0)
	msg1Value := []interface{}{"a", "b", "c"}
	msg2StrVec := make([]interface{}, 0)
	msg2Value := []interface{}{"a", "b", "c"}
	for _, v := range msgs {
		fmt.Print(v.GetSym(), ": ")
		for i := 0; i < v.Size(); i++ {
			fmt.Print(v.GetValue(i).String(), " ")
		}
		fmt.Println()
		if v.GetSym() == "msg1" {
			msg1StrVec = append(msg1StrVec, v.GetValue(2).(*model.Scalar).DataType.Value())
		} else if v.GetSym() == "msg2" {
			msg2StrVec = append(msg2StrVec, v.GetValue(2).(*model.Scalar).DataType.Value())
		}
	}
	assert.Equal(t, msg1Value, msg1StrVec)
	assert.Equal(t, msg2Value, msg2StrVec)

	msg1StrVec = make([]interface{}, 0)
	msg2StrVec = make([]interface{}, 0)
	for _, v := range msgs {
		if v.GetSym() == "msg1" {
			msg1StrVec = append(msg1StrVec, v.GetValueByName("sym").(*model.Scalar).DataType.Value())
		} else if v.GetSym() == "msg2" {
			msg2StrVec = append(msg2StrVec, v.GetValueByName("sym").(*model.Scalar).DataType.Value())
		}
	}
	assert.Equal(t, msg1Value, msg1StrVec)
	assert.Equal(t, msg2Value, msg2StrVec)
}


func TestPollingClientStreamDeserializerErr(t *testing.T) {
	host := "localhost:8848";
	db, err := api.NewDolphinDBClient(context.TODO(), host, nil)
	util.AssertNil(err)

	err = db.Connect()
	util.AssertNil(err)

    loginReq := &api.LoginRequest{
        UserID:   "admin",
        Password: "123456",
    }
    err = db.Login(loginReq)
	util.AssertNil(err)

	_,err = db.RunScript(scripts)
	util.AssertNil(err)

	sdMap := make(map[string][2]string)
	sdMap["msg1"] = [2]string{"", "pt1"}
	sdMap["msg2"] = [2]string{"", "pt2"}

	opt := StreamDeserializerOption {
		TableNames: sdMap,
		Conn:       db,
	}
	sd, err := NewStreamDeserializer(&opt)
	util.AssertNil(err)

	req := &SubscribeRequest{
		Address:    "localhost:8848",
		TableName:  "outTables",
		ActionName: "action1",
		MsgAsTable: true,
		Offset:     0,
		Reconnect:  true,
		MsgDeserializer:  sd,
	}
	client := NewPollingClient("localhost", 8848)

	_, err = client.Subscribe(req)
	assert.EqualError(t, err, "if MsgAsTable is true, MsgDeserializer must be nil")
}