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

type poolHandler struct {
	times int
	msgs []IMessage
}

func (s *poolHandler) DoEvent(msg IMessage) {
	// do something
	s.times += 1
	s.msgs = append(s.msgs, msg)
}

func TestBasicGoroutinePooledClient(t *testing.T) {
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

	tpc := NewGoroutinePooledClient(localhost, 8848)
	sh := poolHandler{}
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

	err = tpc.Subscribe(req)
	util.AssertNil(err)

	time.Sleep(time.Duration(1)*time.Second)
	assert.Equal(t, 6, sh.times)
	for _, v := range sh.msgs {
		assert.Equal(t, model.DtBlob, v.GetValueByName("blob").GetDataType())
		assert.Equal(t, model.DtSymbol, v.GetValueByName("sym").GetDataType())
		assert.Equal(t, model.DtTimestamp, v.GetValueByName("timestampv").GetDataType())
	}
	tpc.Close()
}

type batchPoolHandler struct {
	lines int
	times int
	msgs []IMessage
}

func (s *batchPoolHandler) DoEvent(msg []IMessage) {
	// do something
	s.times += 1
	s.lines += len(msg)
	s.msgs = append(s.msgs, msg...)
}

func TestBatchGoroutinePooledClient(t *testing.T) {
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

	tpc := NewGoroutinePooledClient(localhost, 8848)
	sh := batchPoolHandler{}
	req := &SubscribeRequest{
		Address:    "localhost:8848",
		TableName:  "outTables",
		ActionName: "action1",
		MsgAsTable: false,
		BatchHandler:    &sh,
		Offset:     0,
		Reconnect:  true,
	}
	// req.SetThrottle(100)
	req.SetBatchSize(6)

	err = tpc.Subscribe(req)
	util.AssertNil(err)

	time.Sleep(time.Duration(3)*time.Second)
	assert.Equal(t, 6, sh.lines)
	assert.Equal(t, 1, sh.times)
	tpc.Close()
}

func TestBatchMsgAsTableGoroutinePooledClient(t *testing.T) {
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

	tpc := NewGoroutinePooledClient(localhost, 8848)
	sh := poolHandler{}
	throttle := float32(0.001)
	req := &SubscribeRequest{
		Address:    "localhost:8848",
		TableName:  "outTables",
		ActionName: "action1",
		MsgAsTable: true,
		Handler:    &sh,
		Offset:     0,
		Reconnect:  true,
		Throttle: &throttle,
	}
	req.SetBatchSize(2)

	err = tpc.Subscribe(req)
	util.AssertNil(err)

	time.Sleep(time.Duration(3)*time.Second)
	assert.Equal(t, 3, sh.times)
	tbl := sh.msgs[0]
	assert.Equal(t, 2, tbl.Size())
	tbl = sh.msgs[1]
	assert.Equal(t, 2, tbl.Size())
	assert.Equal(t, model.DfVector, tbl.GetValueByName("blob").GetDataForm())
	tpc.Close()
}

func TestBatchHandlerErrGoroutinePooledClient(t *testing.T) {
	tpc := NewGoroutinePooledClient(localhost, 8848)
	sh := batchPoolHandler{}
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

	err := tpc.Subscribe(req)
	assert.EqualError(t, err, "if BatchSize is not set, the callback in Handler will be called, so it shouldn't be nil")
}
func TestBatchHandlerMsgAsTableErrGoroutinePooledClient(t *testing.T) {
	tpc := NewGoroutinePooledClient(localhost, 8848)
	sh := batchPoolHandler{}
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
	req.SetThrottle(1000)

	err := tpc.Subscribe(req)
	assert.EqualError(t, err, "if MsgAsTable is true, the callback in Handler will be called, so it shouldn't be nil")
}

func TestBatchHandlerNilErrGoroutinePooledClient(t *testing.T) {
	tpc := NewGoroutinePooledClient(localhost, 8848)
	sh := poolHandler{}
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

	err := tpc.Subscribe(req)
	assert.EqualError(t, err, "if BatchSize >= 1 and MsgAsTable is false, the callback in BatchHandler will be called, so it shouldn't be nil")
}

func TestGoroutinePooledClient(t *testing.T) {
	tpc := NewGoroutinePooledClient(localhost, 2888)

	req := &SubscribeRequest{
		Address:    testAddr,
		TableName:  "pooled",
		ActionName: "action1",
		Offset:     0,
		Reconnect:  true,
	}

	req.SetBatchSize(10).SetThrottle(1)

	err := tpc.Subscribe(req)
	assert.Nil(t, err)

	sendMoreSubscription(2888)

	sub := tpc.getSubscriber()
	assert.Equal(t, sub.listeningPort, int32(2888))
	assert.Equal(t, strings.HasPrefix(sub.listeningHost, "127.0.0.1"), true)

	item := &reconnectItem{
		reconnectState:         1,
		lastReconnectTimestamp: time.Now().UnixNano() / 1000000,
	}

	item.putTopic("127.0.0.1:3000:local3000/pooled/action1")
	reconnectTable.Store("127.0.0.1:3000:local3000", item)

	time.Sleep(3 * time.Second)

	err = tpc.UnSubscribe(req)
	assert.Nil(t, err)

	req.ActionName = failedAction
	err = tpc.Subscribe(req)
	assert.Equal(t, err.Error(), "client error response. @K")
	tpc.Close()
}
