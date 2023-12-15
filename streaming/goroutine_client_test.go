package streaming

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/example/util"
	"github.com/dolphindb/api-go/model"
	"github.com/stretchr/testify/assert"
)

var failedAction = "failedAction"

var scripts = "st1 = streamTable(100:0, `timestampv`sym`blob,[TIMESTAMP,SYMBOL,BLOB]);" +
			"share st1 as outTables;" +
			"n = 3;" +
			"table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
			"share table1 as pt1;" +
			"table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
			"share table2 as pt2;" +
			"tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
			"tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
			"d = dict(['msg1','msg2'], [table1, table2]);" +
			"replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv);"

type basicHandler struct {
	times int
	msgs []IMessage
}

func (s *basicHandler) DoEvent(msg IMessage) {
	s.times += 1
	s.msgs = append(s.msgs, msg)
}

type batchHandler struct {
	times int
	lines int
}

func (s *batchHandler) DoEvent(msg []IMessage) {
	s.times += 1
	s.lines += len(msg)
}

func TestBasicGoroutineClient(t *testing.T) {
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

	client := NewGoroutineClient("localhost", 8848)

	sh := basicHandler{}
	throttle := float32(1.1)
	req := &SubscribeRequest{
		Address:    "localhost:8848",
		TableName:  "outTables",
		ActionName: "action1",
		MsgAsTable: false,
		Handler:    &sh,
		Offset:     0,
		Reconnect:  true,
		Throttle: &throttle,
	}

	err = client.Subscribe(req)
	util.AssertNil(err)

	time.Sleep(time.Duration(1)*time.Second)
	assert.Equal(t, 6, sh.times)
	for _, v := range sh.msgs {
		assert.Equal(t, model.DtBlob, v.GetValueByName("blob").GetDataType())
		assert.Equal(t, model.DtSymbol, v.GetValueByName("sym").GetDataType())
		assert.Equal(t, model.DtTimestamp, v.GetValueByName("timestampv").GetDataType())
	}
}

func TestMsgAsTableGoroutineClient(t *testing.T) {
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

	client := NewGoroutineClient("localhost", 8848)

	sh := basicHandler{}
	batch := 6
	req := &SubscribeRequest{
		Address:    "localhost:8848",
		TableName:  "outTables",
		ActionName: "action1",
		MsgAsTable: true,
		Handler:    &sh,
		Offset:     0,
		BatchSize: &batch,
		Reconnect:  true,
	}

	err = client.Subscribe(req)
	util.AssertNil(err)

	time.Sleep(time.Duration(1)*time.Second)
	assert.Equal(t, 1, sh.times)
	tbl := sh.msgs[0]
	assert.Equal(t, 6, tbl.Size())
	symVec := tbl.GetValueByName("sym").(*model.Vector).GetRawValue()
	for _,v := range symVec {
		assert.True(t, v=="msg1" || v=="msg2")
	}
	assert.Equal(t, model.DfVector, tbl.GetValueByName("blob").GetDataForm())
}

func TestBatchGoroutineClient(t *testing.T) {
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

	client := NewGoroutineClient("localhost", 8848)

	sh := batchHandler{}
	throttle := 1
	req := &SubscribeRequest{
		Address:    "localhost:8848",
		TableName:  "outTables",
		ActionName: "action1",
		MsgAsTable: false,
		BatchHandler:    &sh,
		Offset:     0,
		Reconnect:  true,
	}
	req.SetBatchSize(10).SetThrottle(float32(throttle))

	err = client.Subscribe(req)
	util.AssertNil(err)

	time.Sleep(time.Duration(1)*time.Second)
	assert.Equal(t, 1, sh.times)
	assert.Equal(t, 6, sh.lines)
}

func TestErrMsgAsTableWithNoBatch(t *testing.T) {
	client := NewGoroutineClient("localhost", 8848)

	sh := batchHandler{}
	throttle := float32(1)
	req := &SubscribeRequest{
		Address:    "localhost:8848",
		TableName:  "outTables",
		ActionName: "action1",
		MsgAsTable: true,
		BatchHandler:    &sh,
		Offset:     0,
		Reconnect:  true,
		Throttle: &throttle,
	}
	req.SetBatchSize(10)

	err := client.Subscribe(req)
	assert.EqualError(t, err, "if MsgAsTable is true, the callback in Handler will be called, so it shouldn't be nil")
}

func TestErrBasicWithBatch(t *testing.T) {
	client := NewGoroutineClient("localhost", 8848)

	sh := batchHandler{}
	throttle := float32(1)
	req := &SubscribeRequest{
		Address:    "localhost:8848",
		TableName:  "outTables",
		ActionName: "action1",
		MsgAsTable: false,
		BatchHandler:    &sh,
		Offset:     0,
		Reconnect:  true,
		Throttle: &throttle,
	}

	err := client.Subscribe(req)
	assert.EqualError(t, err, "if BatchSize is not set, the callback in Handler will be called, so it shouldn't be nil")
}

func TestErrBasicWithNoBatch(t *testing.T) {
	client := NewGoroutineClient("localhost", 8848)

	sh := basicHandler{}
	throttle := float32(1)
	req := &SubscribeRequest{
		Address:    "localhost:8848",
		TableName:  "outTables",
		ActionName: "action1",
		MsgAsTable: false,
		Handler:    &sh,
		Offset:     0,
		Reconnect:  true,
		Throttle: &throttle,
	}
	req.SetBatchSize(10)

	err := client.Subscribe(req)
	assert.EqualError(t, err, "if BatchSize >= 1 and MsgAsTable is false, the callback in BatchHandler will be called, so it shouldn't be nil")
}

func TestGoroutineClient(t *testing.T) {
	tc := NewGoroutineClient(localhost, 3888)

	req := &SubscribeRequest{
		Address:    testAddr,
		TableName:  "threaded",
		ActionName: "action1",
		Offset:     0,
		Reconnect:  true,
	}

	req.SetBatchSize(10).SetThrottle(float32(1))

	err := tc.Subscribe(req)
	assert.Nil(t, err)

	sendOneSubscription(3888)

	sub := tc.getSubscriber()
	assert.Equal(t, sub.listeningPort, int32(3888))
	assert.Equal(t, strings.HasPrefix(sub.listeningHost, "127.0.0.1"), true)

	item := &reconnectItem{
		reconnectState:         1,
		lastReconnectTimestamp: time.Now().UnixNano() / 1000000,
	}

	item.putTopic("127.0.0.1:3000:local3000/threaded/action1")
	reconnectTable.Store("127.0.0.1:3000:local3000", item)

	time.Sleep(4 * time.Second)

	err = tc.UnSubscribe(req)
	assert.Nil(t, err)

	req.ActionName = failedAction
	err = tc.Subscribe(req)
	assert.Equal(t, err.Error(), "client error response. @K")
	tc.Close()

	time.Sleep(2 * time.Second)
}
