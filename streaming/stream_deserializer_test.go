package streaming

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/example/util"
	"github.com/dolphindb/api-go/model"
	"github.com/stretchr/testify/assert"
)

type sampleHandler struct {
	sd StreamDeserializer
	msgs []IMessage
}

func (s *sampleHandler) DoEvent(msg IMessage) {
	ret, err := s.sd.Parse(msg)
	util.AssertNil(err)
	s.msgs = append(s.msgs, ret)
}

func TestStreamDeserializerInHandler(t *testing.T) {
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

	_,err = db.RunScript(scripts)  //script defined in goroutine_client_test.go
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

	client := NewGoroutineClient("localhost", 8848)

	sh := sampleHandler{*sd, make([]IMessage, 0)}
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

	err = client.Subscribe(req)
	util.AssertNil(err)

	time.Sleep(time.Duration(2)*time.Second)
	assert.Equal(t, 6, len(sh.msgs))

	msg1StrVec := make([]interface{}, 0)
	msg1Value := []interface{}{"a", "b", "c"}
	msg2StrVec := make([]interface{}, 0)
	msg2Value := []interface{}{"a", "b", "c"}
	for _, v := range sh.msgs {
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
	for _, v := range sh.msgs {
		fmt.Print(v.GetSym(), ": ")
		for i := 0; i < v.Size(); i++ {
			fmt.Print(v.GetValue(i).String(), " ")
		}
		fmt.Println()
		if v.GetSym() == "msg1" {
			msg1StrVec = append(msg1StrVec, v.GetValueByName("sym").(*model.Scalar).DataType.Value())
		} else if v.GetSym() == "msg2" {
			msg2StrVec = append(msg2StrVec, v.GetValueByName("sym").(*model.Scalar).DataType.Value())
		}
	}
	assert.Equal(t, msg1Value, msg1StrVec)
	assert.Equal(t, msg2Value, msg2StrVec)
}

type basicHandlerWithoutStreamDeserializer struct {
	msgs []IMessage
}

func (s *basicHandlerWithoutStreamDeserializer) DoEvent(msg IMessage) {
	s.msgs = append(s.msgs, msg)
}

func TestPassInStreamDeserializer(t *testing.T) {
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

	_,err = db.RunScript(scripts)  //script defined in goroutine_client_test.go
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

	client := NewGoroutineClient("localhost", 8848)

	sh := basicHandlerWithoutStreamDeserializer{make([]IMessage, 0)}
	throttle := float32(1)
	req := &SubscribeRequest{
		Address:    "localhost:8848",
		TableName:  "outTables",
		ActionName: "action1",
		MsgAsTable: false,
		Handler:    &sh,
		Offset:     0,
		Reconnect:  true,
		Throttle: 	&throttle,
		MsgDeserializer:  sd,
	}

	err = client.Subscribe(req)
	util.AssertNil(err)

	time.Sleep(time.Duration(1)*time.Second)
	assert.Equal(t, len(sh.msgs), 6)
	msg1StrVec := make([]interface{}, 0)
	msg1Value := []interface{}{"a", "b", "c"}
	msg2StrVec := make([]interface{}, 0)
	msg2Value := []interface{}{"a", "b", "c"}
	for _, v := range sh.msgs {
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
	for _, v := range sh.msgs {
		if v.GetSym() == "msg1" {
			msg1StrVec = append(msg1StrVec, v.GetValueByName("sym").(*model.Scalar).DataType.Value())
		} else if v.GetSym() == "msg2" {
			msg2StrVec = append(msg2StrVec, v.GetValueByName("sym").(*model.Scalar).DataType.Value())
		}
	}
	assert.Equal(t, msg1Value, msg1StrVec)
	assert.Equal(t, msg2Value, msg2StrVec)
}


type batchHandlerWithoutStreamDeserializer struct {
	msgs []IMessage
}

func (s *batchHandlerWithoutStreamDeserializer) DoEvent(msg []IMessage) {
	s.msgs = append(s.msgs, msg...)
}

func TestPassInStreamDeserializerInBatch(t *testing.T) {
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

	_,err = db.RunScript(scripts)  //script defined in goroutine_client_test.go
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

	client := NewGoroutineClient("localhost", 8848)

	sh := batchHandlerWithoutStreamDeserializer{make([]IMessage, 0)}
	throttle := float32(1)
	batch := 2
	req := &SubscribeRequest{
		Address:    "localhost:8848",
		TableName:  "outTables",
		ActionName: "action1",
		MsgAsTable: false,
		BatchHandler:    &sh,
		Offset:     0,
		Reconnect:  true,
		Throttle: 	&throttle,
		BatchSize: &batch,
		MsgDeserializer:  sd,
	}

	err = client.Subscribe(req)
	util.AssertNil(err)

	time.Sleep(time.Duration(2)*time.Second)
	assert.Equal(t, len(sh.msgs), 6)
	msg1StrVec := make([]interface{}, 0)
	msg1Value := []interface{}{"a", "b", "c"}
	msg2StrVec := make([]interface{}, 0)
	msg2Value := []interface{}{"a", "b", "c"}
	for _, v := range sh.msgs {
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
	for _, v := range sh.msgs {
		if v.GetSym() == "msg1" {
			msg1StrVec = append(msg1StrVec, v.GetValueByName("sym").(*model.Scalar).DataType.Value())
		} else if v.GetSym() == "msg2" {
			msg2StrVec = append(msg2StrVec, v.GetValueByName("sym").(*model.Scalar).DataType.Value())
		}
	}
	assert.Equal(t, msg1Value, msg1StrVec)
	assert.Equal(t, msg2Value, msg2StrVec)
}


type poolDeserializerHandler struct {
	times int
	msgs []IMessage
}

func (s *poolDeserializerHandler) DoEvent(msg IMessage) {
	// do something
	s.times += 1
	s.msgs = append(s.msgs, msg)
}


func TestPoolStreamDeserializer(t *testing.T) {
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

	sdMap := make(map[string][2]string)
	sdMap["msg1"] = [2]string{"", "pt1"}
	sdMap["msg2"] = [2]string{"", "pt2"}

	opt := StreamDeserializerOption {
		TableNames: sdMap,
		Conn:       db,
	}
	sd, err := NewStreamDeserializer(&opt)
	util.AssertNil(err)

	tpc := NewGoroutinePooledClient(localhost, 8848)
	sh := poolDeserializerHandler{}
	throttle := float32(0.000)
	req := &SubscribeRequest{
		Address:    "localhost:8848",
		TableName:  "outTables",
		ActionName: "action1",
		MsgAsTable: false,
		Handler:    &sh,
		Offset:     0,
		MsgDeserializer: sd,
		Reconnect:  true,
		Throttle: &throttle,
	}
	req.SetBatchSize(0)

	err = tpc.Subscribe(req)
	util.AssertNil(err)

	time.Sleep(time.Duration(3)*time.Second)

	assert.Equal(t, len(sh.msgs), 6)
	msg1StrVec := make([]interface{}, 0)
	msg1Value := []interface{}{"a", "b", "c"}
	msg2StrVec := make([]interface{}, 0)
	msg2Value := []interface{}{"a", "b", "c"}
	for _, v := range sh.msgs {
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
	for _, v := range sh.msgs {
		if v.GetSym() == "msg1" {
			msg1StrVec = append(msg1StrVec, v.GetValueByName("sym").(*model.Scalar).DataType.Value())
		} else if v.GetSym() == "msg2" {
			msg2StrVec = append(msg2StrVec, v.GetValueByName("sym").(*model.Scalar).DataType.Value())
		}
	}
	assert.Equal(t, msg1Value, msg1StrVec)
	assert.Equal(t, msg2Value, msg2StrVec)
}


var arrayVectorReplayScript = "st1 = streamTable(100:0, `timestampv`sym`blob,[TIMESTAMP,SYMBOL,BLOB]);" +
	"share st1 as outTables;" +
	"n = 300;" +
	"table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, INT[], DOUBLE]);" +
	"share table1 as pt1;" +
	"table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, LONG[]]);" +
	"share table2 as pt2;" +
	"tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), arrayVector(9*(1..n), 1..(9*n)), take(100.0, n));" +
	"tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), arrayVector(8*(1..n), 1..(8*n)));" +
	"d = dict(['msg1','msg2'], [table1, table2]);" +
	"submitJob(`replay,`replay,replay, d, `outTables, `timestampv, `timestampv, 7, false, 1);"
	// "sleep(1000*5);" +
	// "(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv,replayRate=2);"

var endScript = "tableInsert(outTables, table([2012.01.01T01:21:23] as timestampv, [`end] as sym, [`blob] as blob));"

type replayHandle struct {
	sd   StreamDeserializer
	msgs1 []IMessage
	msgs2 []IMessage
	ch 	 chan bool
}

func (s *replayHandle) DoEvent(msgs []IMessage) {
	for _, msg := range msgs {
		fmt.Println(msg.GetValue(1).GetDataTypeString(), msg.GetValue(1).GetDataFormString(), msg)
		fmt.Println(msg.GetValue(1))
		if msg.GetValue(1).(*model.Scalar).DataType.String() == "end" {
			fmt.Println("DoEvent: got to end")
			s.ch <- true;
			return
		}
		ret, err := s.sd.Parse(msg)
		util.AssertNil(err)
		if(msg.GetValue(1).(*model.Scalar).DataType.String() == "msg1") {
			fmt.Println(ret.GetValue(3), ret.GetValue(3), ret.GetValue(4), " ")
			arrV := ret.GetValue(3)
			fmt.Println(arrV.GetDataType(), arrV.GetDataTypeString())
			s.msgs1 = append(s.msgs1, ret)
		} else {
			s.msgs2 = append(s.msgs2, ret)
		}
	}
}
func TestStreamDeserializerReplayParam(t *testing.T) {
	host := "localhost:8848"
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

	_, err = db.RunScript(arrayVectorReplayScript) //script defined in goroutine_client_test.go
	util.AssertNil(err)


	sdMap := make(map[string][2]string)
	sdMap["msg1"] = [2]string{"", "pt1"}
	sdMap["msg2"] = [2]string{"", "pt2"}

	opt := StreamDeserializerOption{
		TableNames: sdMap,
		Conn:       db,
	}
	sd, err := NewStreamDeserializer(&opt)
	util.AssertNil(err)

	client := NewGoroutineClient("localhost", 12345)

	sh := replayHandle{*sd, make([]IMessage, 0), make([]IMessage, 0), make(chan bool)}
	batchSize := 10
	throttle := float32(1)
	req := &SubscribeRequest{
		Address:    "localhost:8848",
		TableName:  "outTables",
		ActionName: "action1",
		MsgAsTable: false,
		BatchHandler:    &sh,
		Offset:     0,
		Reconnect:  true,
		Throttle:   &throttle,
		BatchSize:  &batchSize,
	}
	err = client.Subscribe(req)
	util.AssertNil(err)

	time.Sleep(time.Duration(3)*time.Second)

	_, err = db.RunScript(endScript) //script defined in goroutine_client_test.go
	util.AssertNil(err)

	<- sh.ch

	assert.Equal(t, 300, len(sh.msgs1))
	assert.Equal(t, 300, len(sh.msgs2))
}


func TestStreamBug(t *testing.T) {
	host := "localhost:8848"
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

	_, err = db.RunScript(arrayVectorReplayScript) //script defined in goroutine_client_test.go
	util.AssertNil(err)

	// time.Sleep(time.Duration(3)*time.Second)
	_, err = db.RunScript(endScript) //script defined in goroutine_client_test.go
	util.AssertNil(err)
}