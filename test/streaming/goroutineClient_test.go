package test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/streaming"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

var gcConn, _ = api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
var stopLabel bool
var wg sync.WaitGroup

func threadWriteData(tabName string) {
	defer wg.Done()
	for {
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + tabName + ".append!(t)")
		AssertNil(err)
		if stopLabel {
			break
		}
	}
}

func waitData(tableName string, dataRow int) {
	loop := 0
	for {
		loop += 1
		if loop > 60 {
			panic("wait for subscribe datas timeout.")
		}
		tmp, err := gcConn.RunScript("exec count(*) from " + tableName)
		AssertNil(err)
		rowNum := tmp.(*model.Scalar)
		fmt.Printf("\nexpectedData is: %v", dataRow)
		fmt.Printf("\nactualData is: %v", rowNum)
		if dataRow == int(rowNum.Value().(int32)) {
			break
		}
		time.Sleep(1 * time.Second)
	}
}

type MessageBatchHandler struct {
	receive string
}

func (s *MessageBatchHandler) DoEvent(msgv []streaming.IMessage) {
	for _, msg := range msgv {
		val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
		val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
		val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
		script := fmt.Sprintf("tableInsert(objByName(`"+s.receive+", true), %s,%s,%s)",
			val0, val1, val2)
		_, err := gcConn.RunScript(script)
		AssertNil(err)
	}
}

type MessageHandler struct {
	receive string
}

func (s *MessageHandler) DoEvent(msg streaming.IMessage) {
	val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
	val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
	script := fmt.Sprintf("tableInsert(objByName(`"+s.receive+", true), %s,%s,%s)",
		val0, val1, val2)
	_, err := gcConn.RunScript(script)
	AssertNil(err)
}

type MessageHandler_table struct {
	receive string
}

func (s *MessageHandler_table) DoEvent(msg streaming.IMessage) {
	val0 := msg.GetValue(0).(*model.Vector)
	val1 := msg.GetValue(1).(*model.Vector)
	val2 := msg.GetValue(2).(*model.Vector)

	for i := 0; i < len(val0.Data.Value()); i++ {
		script := fmt.Sprintf("tableInsert(objByName(`"+s.receive+", true), %s,%s,%s)",
			val0.Data.Get(i).String(), val1.Data.Get(i).String(), val2.Data.Get(i).String())
		_, err := gcConn.RunScript(script)
		AssertNil(err)
	}
}

func TestGoroutineClient_bachSize_throttle(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("test_NewGoroutinePooledClient_batchSize_lt0", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageHandler{
			receive: receive,
		}
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  st,
			ActionName: "action1",
			Offset:     0,
			Reconnect:  true,
			Handler:    &handler,
		}
		req.SetBatchSize(-1)
		err := gc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		for {
			res, err := gcConn.RunScript("exec * from " + receive)
			So(err, ShouldBeNil)
			if res.Rows() == 1000 {
				break
			}
		}

		err = gc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	Convey("test_NewGoroutinePooledClient_throttle_less_than_0", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler{
			receive: receive,
		}
		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "action1",
			Offset:       0,
			Reconnect:    true,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000).SetThrottle(-2)
		err := gc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		for {
			res, err := gcConn.RunScript("exec * from " + receive)
			So(err, ShouldBeNil)
			if res.Rows() == 1000 {
				break
			}
		}
		err = gc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_tableName_offset(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_tableName_offset", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler{
			receive: receive,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "action1",
			Offset:       0,
			Reconnect:    false,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(receive, 2000)
		reTmp, err := gcConn.RunScript(receive)
		So(err, ShouldBeNil)
		exTmp, err := gcConn.RunScript(st)
		So(err, ShouldBeNil)
		re := reTmp.(*model.Table)
		ex := exTmp.(*model.Table)
		CheckmodelTableEqual(re, ex, 0)
		err = gc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_tableName_actionName(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_tableName_actionName", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler{
			receive: receive,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTrades1",
			Offset:       0,
			Reconnect:    false,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(receive, 4000)
		reTmp, err := gcConn.RunScript(receive)
		So(err, ShouldBeNil)
		exTmp, err := gcConn.RunScript(st)
		So(err, ShouldBeNil)
		re := reTmp.(*model.Table)
		ex := exTmp.(*model.Table)
		So(re.Rows(), ShouldEqual, 4000)
		CheckmodelTableEqual(re, ex, 0)
		err = gc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_tableName_handler_offset_reconnect_success(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_tableName_handler_offset_reconnect_success", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageHandler_r{
			receive: receive,
		}
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:   setup.Address,
			TableName: st,
			Offset:    0,
			Reconnect: true,
			Handler:   &handler,
		}
		err = gc.Subscribe(req)
		So(err, ShouldBeNil)

		_, err = gcConn_r.RunScript("n=500;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		_, err = gcConn_r.RunScript("stopPublishTable('" + setup.IP + "'," + strconv.Itoa(setup.Port) + ",'" + st + "')")
		So(err, ShouldBeNil)

		_, err = gcConn_r.RunScript("n=500;t=table(1..n+500 as tag,now()+1..n+500 as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		_, err = gcConn_r.RunScript("stopPublishTable('" + setup.IP + "'," + strconv.Itoa(setup.Port) + ",'" + st + "')")
		So(err, ShouldBeNil)

		time.Sleep(10 * time.Second)
		res, _ := gcConn_r.RunScript("res = select * from " + receive + " order by tag;ex = select * from " + st + " order by tag;each(eqObj, ex.values(), res.values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}

		err = gc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handlereconnect(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handlereconnect", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler{
			receive: receive,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTrades1",
			Reconnect:    true,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gc.Subscribe(req)
		So(err, ShouldBeNil)
		wg.Add(1)
		go threadWriteData(st)
		time.Sleep(2 * time.Second)
		_, err = gcConn.RunScript("stopPublishTable('" + setup.IP + "'," + strconv.Itoa(setup.Port) + ",'" + st + "')")
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		_, err = gcConn.RunScript("stopPublishTable('" + setup.IP + "'," + strconv.Itoa(setup.Port) + ",'" + st + "')")
		So(err, ShouldBeNil)
		rowNum1, err := gcConn.RunScript("(exec count(*) from " + receive + ")[0]")
		So(err, ShouldBeNil)
		reRowNum1 := rowNum1.(*model.Scalar)
		time.Sleep(3 * time.Second)
		rowNum2, err := gcConn.RunScript("(exec count(*) from " + receive + ")[0]")
		So(err, ShouldBeNil)
		reRowNum2 := rowNum2.(*model.Scalar)
		stopLabel = true
		wg.Wait()
		So(reRowNum2.Value(), ShouldBeGreaterThanOrEqualTo, reRowNum1.Value())
		err = gc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_0(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_0", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler{
			receive: receive,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTrades1",
			Offset:       0,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(receive, 2000)
		tmp1, err := gcConn.RunScript(receive)
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		tmp2, err := gcConn.RunScript(st)
		So(err, ShouldBeNil)
		ex := tmp2.(*model.Table)
		CheckmodelTableEqual(re, ex, 0)
		err = gc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_negative(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_negative", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler{
			receive: receive,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,2020.01.04T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTrades1",
			Offset:       -3,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=1000;t=table(1..n as tag,2020.01.01T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=1000;t=table(1..n as tag,2020.01.02T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=1000;t=table(1..n as tag,2020.01.03T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(receive, 3000)
		tmp1, err := gcConn.RunScript(receive)
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		tmp2, err := gcConn.RunScript("select * from " + st + " where rowNo(tag)>=1000")
		So(err, ShouldBeNil)
		ex := tmp2.(*model.Table)
		So(re.Rows(), ShouldEqual, 3000)
		CheckmodelTableEqual(re, ex, 0)
		err = gc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_10(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_10", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler{
			receive: receive,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,2020.01.04T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTradesOffset",
			Offset:       10,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=1000;t=table(1..n as tag,2020.01.01T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=1000;t=table(1..n as tag,2020.01.02T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=1000;t=table(1..n as tag,2020.01.03T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(receive, 3990)
		tmp1, err := gcConn.RunScript(receive)
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		tmp2, err := gcConn.RunScript("select * from " + st + " where rowNo(tag)>=10")
		So(err, ShouldBeNil)
		ex := tmp2.(*model.Table)
		So(re.Rows(), ShouldEqual, 3990)
		CheckmodelTableEqual(re, ex, 0)
		err = gc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_offset_morethan_tableCount(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_offset_morethan_tableCount", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler{
			receive: receive,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,2020.01.04T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTrades1",
			Offset:       1000,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gc.Subscribe(req)
		So(err, ShouldBeNil)
		time.Sleep(3 * time.Second)
		tmp1, err := gcConn.RunScript(receive)
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		So(re.Rows(), ShouldEqual, 0)
		err = gc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_filter(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_filter", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageHandler{
			receive: receive,
		}
		filter1, err := gcConn.RunScript("1..1000")
		So(err, ShouldBeNil)
		filter2, err := gcConn.RunScript("2001..3000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  st,
			ActionName: "subTrades1",
			Offset:     0,
			Filter:     filter1.(*model.Vector),
			Handler:    &handler,
		}
		req2 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  st,
			ActionName: "subTrades2",
			Offset:     0,
			Filter:     filter2.(*model.Vector),
			Handler:    &handler,
		}
		err = gc.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=4000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		tmp1, err := gcConn.RunScript(receive)
		So(err, ShouldBeNil)
		waitData(receive, 1000)
		tmp3, err := gcConn.RunScript(receive)
		So(err, ShouldBeNil)
		So(tmp3.Rows(), ShouldEqual, 1000)
		CheckmodelTableEqual(tmp1.(*model.Table), tmp3.(*model.Table), 1000)

		err = gc.Subscribe(req2)
		So(err, ShouldBeNil)
		waitData(receive, 1000)
		tmp3, err = gcConn.RunScript(receive)
		So(err, ShouldBeNil)
		So(tmp3.Rows(), ShouldEqual, 1000)
		CheckmodelTableEqual(tmp1.(*model.Table), tmp3.(*model.Table), 1000)
		err = gc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		err = gc.UnSubscribe(req2)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)

	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_batchSize_throttle(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_batchSize_throttle", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler{
			receive: receive,
		}
		req1 := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTrades1",
			Offset:       -1,
			BatchHandler: &handler,
			Reconnect:    true,
		}
		req1.SetBatchSize(10000).SetThrottle(5)
		err := gc.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(receive, 10000)
		tmp1, err := gcConn.RunScript(receive)
		So(err, ShouldBeNil)
		err = gc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		re1 := tmp1.(*model.Table)
		So(re1.Rows(), ShouldEqual, 10000)
		res, _ := gcConn.RunScript("each(eqObj, " + st + ".values(), " + receive + ".values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_batchSize_throttle2(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_batchSize_throttle2", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler{
			receive: receive,
		}
		filter1, err := gcConn.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTrades1",
			Offset:       -1,
			Filter:       filter1.(*model.Vector),
			BatchHandler: &handler,
			Reconnect:    true,
		}
		req1.SetBatchSize(10000).SetThrottle(5)
		err = gc.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(receive, 200)
		tmp1, err := gcConn.RunScript(receive)
		So(err, ShouldBeNil)
		tmp2, err := gcConn.RunScript(st)
		So(err, ShouldBeNil)
		err = gc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		re1 := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		So(re1.Rows(), ShouldEqual, 200)
		CheckmodelTableEqual(re1, ex, 0)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_unsubscribeesubscribe(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_unsubscribeesubscribe", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler{
			receive: receive,
		}
		filter1, err := gcConn.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTrades1",
			Offset:       -1,
			Filter:       filter1.(*model.Vector),
			BatchHandler: &handler,
			Reconnect:    true,
		}
		req1.SetBatchSize(10000).SetThrottle(5)
		err = gc.Subscribe(req1)
		So(err, ShouldBeNil)
		err = gc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		err = gc.Subscribe(req1)
		So(err, ShouldBeNil)
		err = gc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_offseteconnect_filter_AllowExistTopic(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_offseteconnect_filter_AllowExistTopic", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler{
			receive: receive,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		filter1, err := gcConn.RunScript("1..100000")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTrades1",
			Offset:       0,
			Reconnect:    true,
			Filter:       filter1.(*model.Vector),
			BatchHandler: &handler,
			AllowExists:  true,
		}
		req.SetBatchSize(100).SetThrottle(5)
		err = gc.Subscribe(req)
		So(err, ShouldBeNil)
		wg.Add(1)
		go threadWriteData(st)
		time.Sleep(2 * time.Second)
		_, err = gcConn.RunScript("stopPublishTable('" + setup.IP + "'," + strconv.Itoa(setup.SubPort) + ",'" + st + "', 'subTrades1')")
		So(err, ShouldBeNil)
		rowNum1, err := gcConn.RunScript("(exec count(*) from " + receive + ")[0]")
		So(err, ShouldBeNil)
		reRowNum1 := rowNum1.(*model.Scalar)
		time.Sleep(3 * time.Second)
		rowNum2, err := gcConn.RunScript("(exec count(*) from " + receive + ")[0]")
		So(err, ShouldBeNil)
		reRowNum2 := rowNum2.(*model.Scalar)
		stopLabel = true
		wg.Wait()
		So(reRowNum2.Value(), ShouldBeGreaterThanOrEqualTo, reRowNum1.Value())
		err = gc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_not_contain_handler(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_not_contain_handler_1000", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		req1 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  st,
			ActionName: "subTrades1",
			Offset:     -1,
			Reconnect:  true,
		}
		err := gc.Subscribe(req1)
		So(err.Error(), ShouldContainSubstring, "if BatchSize is not set, the callback in Handler will be called, so it shouldn't be nil")
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_msgAsTable(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_msgAsTable", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageHandler_table{
			receive: receive,
		}
		req1 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  st,
			ActionName: "subTrades1",
			Offset:     0,
			Handler:    &handler,
			Reconnect:  true,
			MsgAsTable: true,
		}
		req1.SetBatchSize(1000)
		err := gc.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		for {
			res, err := gcConn.RunScript("exec * from " + receive)
			So(err, ShouldBeNil)
			if res.Rows() == 1000 {
				break
			}
		}
		err = gc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

type sdHandler1 struct {
	sd         streaming.StreamDeserializer
	msg1_total int
	msg2_total int
	res1_data  []*model.Vector
	res2_data  []*model.Vector
	coltype1   []model.DataTypeByte
	coltype2   []model.DataTypeByte
	lock       *sync.Mutex
}

type sdBatchHandler1 struct {
	sd         streaming.StreamDeserializer
	msg1_total int
	msg2_total int
	res1_data  []*model.Vector
	res2_data  []*model.Vector
	coltype1   []model.DataTypeByte
	coltype2   []model.DataTypeByte
	lock       *sync.Mutex
}

func (s *sdHandler1) DoEvent(msg streaming.IMessage) {
	ret, err := s.sd.Parse(msg)
	AssertNil(err)
	sym := ret.GetSym()

	s.lock.Lock()
	if sym == "msg1" {
		s.msg1_total += 1
		AssertEqual(ret.Size(), 5)
		for i := 0; i < len(s.coltype1); i++ {
			AssertEqual(ret.GetValue(i).GetDataType(), s.coltype1[i])
			// fmt.Println(ret.GetValue(i).(*model.Scalar).Value())
			val := ret.GetValue(i).(*model.Scalar).Value()
			dt, err := model.NewDataType(s.coltype1[i], val)
			AssertNil(err)
			AssertNil(s.res1_data[i].Append(dt))
		}

	} else if sym == "msg2" {
		s.msg2_total += 1
		AssertEqual(ret.Size(), 4)
		for i := 0; i < len(s.coltype2); i++ {
			AssertEqual(ret.GetValue(i).GetDataType(), s.coltype2[i])
			// fmt.Println(ret.GetValue(i).GetDataType(), ex_types2[i])
			val := ret.GetValue(i).(*model.Scalar).Value()
			dt, err := model.NewDataType(s.coltype2[i], val)
			AssertNil(err)
			AssertNil(s.res2_data[i].Append(dt))
		}
	}
	s.lock.Unlock()
}

func (s *sdBatchHandler1) DoEvent(msgs []streaming.IMessage) {
	for _, msg := range msgs {
		ret, err := s.sd.Parse(msg)
		AssertNil(err)
		sym := ret.GetSym()

		s.lock.Lock()
		if sym == "msg1" {
			s.msg1_total += 1
			AssertEqual(ret.Size(), 5)
			for i := 0; i < len(s.coltype1); i++ {
				AssertEqual(ret.GetValue(i).GetDataType(), s.coltype1[i])
				// fmt.Println(ret.GetValue(i).(*model.Scalar).Value())
				val := ret.GetValue(i).(*model.Scalar).Value()
				dt, err := model.NewDataType(s.coltype1[i], val)
				AssertNil(err)
				AssertNil(s.res1_data[i].Append(dt))
			}

		} else if sym == "msg2" {
			s.msg2_total += 1
			AssertEqual(ret.Size(), 4)
			for i := 0; i < len(s.coltype2); i++ {
				AssertEqual(ret.GetValue(i).GetDataType(), s.coltype2[i])
				// fmt.Println(ret.GetValue(i).GetDataType(), ex_types2[i])
				val := ret.GetValue(i).(*model.Scalar).Value()
				dt, err := model.NewDataType(s.coltype2[i], val)
				AssertNil(err)
				AssertNil(s.res2_data[i].Append(dt))
			}

		}
		s.lock.Unlock()
	}

}

func createStreamDeserializer() (sdHandler1, sdBatchHandler1) {
	_, err := gcConn.RunScript(
		`st2_gc = streamTable(100:0, 'timestampv''sym''blob''price1',[TIMESTAMP,SYMBOL,BLOB,DOUBLE]);
		enableTableShareAndPersistence(table=st2_gc, tableName='SDoutTables_gc', asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0);
		go;
		setStreamTableFilterColumn(SDoutTables_gc, 'sym')`)
	AssertNil(err)
	_, err = gcConn.RunScript(
		`n = 1000;
		t0 = table(100:0, "datetimev""timestampv""sym""price1""price2", [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);
		share t0 as table1_gc;
		t = table(100:0, "datetimev""timestampv""sym""price1", [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);
		tableInsert(table1_gc, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take("a1""b1""c1",n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));
		tableInsert(t, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take("a1""b1""c1",n), rand(100,n)+rand(1.0, n));
		dbpath="dfs://test_dfs";if(existsDatabase(dbpath)){dropDatabase(dbpath)};db=database(dbpath, VALUE, "a1""b1""c1");
		db.createPartitionedTable(t,"table2_gc","sym").append!(t);
		t2 = select * from loadTable(dbpath,"table2_gc");share t2 as table2_gc;
		d = dict(['msg1','msg2'], [table1_gc, table2_gc]);
		replay(inputTables=d, outputTables="SDoutTables_gc", dateColumn="timestampv", timeColumn="timestampv")`)
	AssertNil(err)
	sdMap := make(map[string][2]string)
	sdMap["msg1"] = [2]string{"", "table1_gc"}
	sdMap["msg2"] = [2]string{"dfs://test_dfs", "table2_gc"}
	opt := streaming.StreamDeserializerOption{
		TableNames: sdMap,
		Conn:       gcConn,
	}
	sd, err := streaming.NewStreamDeserializer(&opt)
	AssertNil(err)
	ex_types1 := []model.DataTypeByte{model.DtDatetime, model.DtTimestamp, model.DtSymbol, model.DtDouble, model.DtDouble}
	args1 := make([]*model.Vector, 5)
	args1[0] = model.NewVector(model.NewDataTypeList(ex_types1[0], []model.DataType{}))
	args1[1] = model.NewVector(model.NewDataTypeList(ex_types1[1], []model.DataType{}))
	args1[2] = model.NewVector(model.NewDataTypeList(ex_types1[2], []model.DataType{}))
	args1[3] = model.NewVector(model.NewDataTypeList(ex_types1[3], []model.DataType{}))
	args1[4] = model.NewVector(model.NewDataTypeList(ex_types1[4], []model.DataType{}))
	ex_types2 := []model.DataTypeByte{model.DtDatetime, model.DtTimestamp, model.DtSymbol, model.DtDouble}
	args2 := make([]*model.Vector, 4)
	args2[0] = model.NewVector(model.NewDataTypeList(ex_types2[0], []model.DataType{}))
	args2[1] = model.NewVector(model.NewDataTypeList(ex_types2[1], []model.DataType{}))
	args2[2] = model.NewVector(model.NewDataTypeList(ex_types2[2], []model.DataType{}))
	args2[3] = model.NewVector(model.NewDataTypeList(ex_types2[3], []model.DataType{}))

	var lock1 sync.Mutex
	var lock2 sync.Mutex
	plock1 := &lock1
	plock2 := &lock2
	sh := sdHandler1{*sd, 0, 0, args1, args2, ex_types1, ex_types2, plock1}
	sbh := sdBatchHandler1{*sd, 0, 0, args1, args2, ex_types1, ex_types2, plock2}
	fmt.Println("create handler successfully.")
	return sh, sbh
}

func TestGoroutineClient_subscribe_with_StreamDeserializer(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)

	Convey("TestGoroutineClient_subscribe_onehandler_with_StreamDeserializer", t, func() {
		_, err := gcConn.RunScript(
			"try{ dropStreamTable(`SDoutTables_gc);}catch(ex){};" +
				"try{ dropStreamTable(`st2_gc);}catch(ex){};" +
				"try{ undef(`table1_gc, SHARED);}catch(ex){};" +
				"try{ undef(`table2_gc, SHARED);}catch(ex){};go")
		So(err, ShouldBeNil)
		sdhandler, _ := createStreamDeserializer()
		req1 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "SDoutTables_gc",
			ActionName: "testStreamDeserializer",
			Offset:     0,
			Handler:    &sdhandler,
			Reconnect:  true,
		}

		targetows := 2000
		err = gc.Subscribe(req1)
		So(err, ShouldBeNil)
		fmt.Println("started subscribe...")
		for {
			time.Sleep(1 * time.Second)
			if sdhandler.msg1_total+sdhandler.msg2_total == targetows {
				break
			}
		}
		err = gc.UnSubscribe(req1)
		So(err, ShouldBeNil)

		res_tab1 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1", "price2"}, sdhandler.res1_data)
		res_tab2 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1"}, sdhandler.res2_data)

		gcConn.Upload(map[string]model.DataForm{"res1": res_tab1, "res2": res_tab2})
		ans1, err := gcConn.RunScript("res = select * from res1 order by datetimev,timestampv;ex= select * from table1_gc order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans1.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}

		ans2, err := gcConn.RunScript("res = select * from res2 order by datetimev,timestampv;ex= select * from table2_gc order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans2.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}
		_, err = gcConn.RunScript(
			"try{ dropStreamTable(`SDoutTables_gc);}catch(ex){};" +
				"try{ dropStreamTable(`st2_gc);}catch(ex){};" +
				"try{ undef(`table1_gc, SHARED);}catch(ex){};" +
				"try{ undef(`table2_gc, SHARED);}catch(ex){};go")
		So(err, ShouldBeNil)

	})
	Convey("TestGoroutineClient_subscribe_batchHandler_with_StreamDeserializer", t, func() {
		_, err := gcConn.RunScript(
			"try{ dropStreamTable(`SDoutTables_gc);}catch(ex){};" +
				"try{ dropStreamTable(`st2_gc);}catch(ex){};" +
				"try{ undef(`table1_gc, SHARED);}catch(ex){};" +
				"try{ undef(`table2_gc, SHARED);}catch(ex){};go")
		So(err, ShouldBeNil)
		_, sdBatchHandler1 := createStreamDeserializer()
		req1 := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    "SDoutTables_gc",
			ActionName:   "testStreamDeserializer",
			Offset:       0,
			BatchHandler: &sdBatchHandler1,
			Reconnect:    true,
		}
		req1.SetBatchSize(500)
		targetows := 2000
		err = gc.Subscribe(req1)
		So(err, ShouldBeNil)
		fmt.Println("started subscribe...")
		for {
			time.Sleep(1 * time.Second)
			if sdBatchHandler1.msg1_total+sdBatchHandler1.msg2_total == targetows {
				break
			}
		}
		err = gc.UnSubscribe(req1)
		So(err, ShouldBeNil)

		res_tab1 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1", "price2"}, sdBatchHandler1.res1_data)
		res_tab2 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1"}, sdBatchHandler1.res2_data)

		gcConn.Upload(map[string]model.DataForm{"res1": res_tab1, "res2": res_tab2})
		ans1, err := gcConn.RunScript("res = select * from res1 order by datetimev,timestampv;ex= select * from table1_gc order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans1.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}

		ans2, err := gcConn.RunScript("res = select * from res2 order by datetimev,timestampv;ex= select * from table2_gc order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans2.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}
		_, err = gcConn.RunScript(
			"try{ dropStreamTable(`SDoutTables_gc);}catch(ex){};" +
				"try{ dropStreamTable(`st2_gc);}catch(ex){};" +
				"try{ undef(`table1_gc, SHARED);}catch(ex){};" +
				"try{ undef(`table2_gc, SHARED);}catch(ex){};go")
		So(err, ShouldBeNil)

	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}
