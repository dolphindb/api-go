package test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/streaming"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

var host1 = getRandomClusterAddress()
var gcConn, _ = api.NewSimpleDolphinDBClient(context.TODO(), host1, setup.UserName, setup.Password)

func TestGoroutineClient_bachSize_throttle(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("test_NewGoroutinePooledClient_batchSize_lt0", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		fmt.Println(`222222222222222222222222222222`)
		handler := MessageHandler{
			receive: receive,
			conn:    gcConn,
		}
		req := &streaming.SubscribeRequest{
			Address:    host1,
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
		fmt.Println(`1111111111111111111111111111`)
		So(err, ShouldBeNil)
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	Convey("test_NewGoroutinePooledClient_throttle_less_than_0", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn,
		}
		req := &streaming.SubscribeRequest{
			Address:      host1,
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
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_tableName_offset(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_tableName_offset", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host1,
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
		waitData(gcConn, receive, 2000)
		reTmp, err := gcConn.RunScript(receive)
		So(err, ShouldBeNil)
		exTmp, err := gcConn.RunScript(st)
		So(err, ShouldBeNil)
		re := reTmp.(*model.Table)
		ex := exTmp.(*model.Table)
		CheckmodelTableEqual(re, ex, 0)
		err = gc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_tableName_actionName(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_tableName_actionName", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host1,
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
		waitData(gcConn, receive, 4000)
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
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_tableName_handler_offseteconnect_success(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_tableName_handler_offseteconnect_success", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		handler := MessageHandler{
			receive: receive,
			conn:    gcConn,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:   host1,
			TableName: st,
			Offset:    0,
			Reconnect: true,
			Handler:   &handler,
		}
		err = gc.Subscribe(req)
		So(err, ShouldBeNil)

		_, err = gcConn.RunScript("n=500;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		_, err = gcConn.RunScript("stopPublishTable('" + setup.IP + "'," + strings.Split(host1, ":")[1] + ",'" + st + "')")
		So(err, ShouldBeNil)

		_, err = gcConn.RunScript("n=500;t=table(1..n+500 as tag,now()+1..n+500 as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		_, err = gcConn.RunScript("stopPublishTable('" + setup.IP + "'," + strings.Split(host1, ":")[1] + ",'" + st + "')")
		So(err, ShouldBeNil)

		time.Sleep(10 * time.Second)
		res, _ := gcConn.RunScript("res = select * from " + receive + " order by tag;ex = select * from " + st + " order by tag;each(eqObj, ex.values(), res.values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}

		err = gc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handlereconnect(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handlereconnect", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host1,
			TableName:    st,
			ActionName:   "subTrades1",
			Reconnect:    true,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gc.Subscribe(req)
		So(err, ShouldBeNil)
		wg.Add(1)
		go threadWriteData(gcConn, st, 10)
		time.Sleep(2 * time.Second)
		_, err = gcConn.RunScript("stopPublishTable('" + setup.IP + "'," + strings.Split(host1, ":")[1] + ",'" + st + "')")
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		_, err = gcConn.RunScript("stopPublishTable('" + setup.IP + "'," + strings.Split(host1, ":")[1] + ",'" + st + "')")
		So(err, ShouldBeNil)
		rowNum1, err := gcConn.RunScript("(exec count(*) from " + receive + ")[0]")
		So(err, ShouldBeNil)
		reRowNum1 := rowNum1.(*model.Scalar)
		time.Sleep(3 * time.Second)
		rowNum2, err := gcConn.RunScript("(exec count(*) from " + receive + ")[0]")
		So(err, ShouldBeNil)
		reRowNum2 := rowNum2.(*model.Scalar)
		wg.Wait()
		So(reRowNum2.Value(), ShouldBeGreaterThanOrEqualTo, reRowNum1.Value())
		waitData(gcConn, receive, 11000)
		err = gc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_0(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_0", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host1,
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
		waitData(gcConn, receive, 2000)
		tmp1, err := gcConn.RunScript(receive)
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		tmp2, err := gcConn.RunScript(st)
		So(err, ShouldBeNil)
		ex := tmp2.(*model.Table)
		CheckmodelTableEqual(re, ex, 0)
		err = gc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_negative(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_negative", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,2020.01.04T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host1,
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
		waitData(gcConn, receive, 3000)
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
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_10(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_10", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,2020.01.04T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host1,
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
		waitData(gcConn, receive, 3990)
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
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_offset_morethan_tableCount(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_offset_morethan_tableCount", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,2020.01.04T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host1,
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
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_filter(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_filter", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		handler := MessageHandler{
			receive: receive,
			conn:    gcConn,
		}
		filter1, err := gcConn.RunScript("1..1000")
		So(err, ShouldBeNil)
		filter2, err := gcConn.RunScript("2001..3000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:    host1,
			TableName:  st,
			ActionName: "subTrades1",
			Offset:     0,
			Filter:     filter1.(*model.Vector),
			Handler:    &handler,
		}
		req2 := &streaming.SubscribeRequest{
			Address:    host1,
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
		waitData(gcConn, receive, 1000)
		tmp3, err := gcConn.RunScript(receive)
		So(err, ShouldBeNil)
		So(tmp3.Rows(), ShouldEqual, 1000)
		CheckmodelTableEqual(tmp1.(*model.Table), tmp3.(*model.Table), 1000)

		err = gc.Subscribe(req2)
		So(err, ShouldBeNil)
		waitData(gcConn, receive, 1000)
		tmp3, err = gcConn.RunScript(receive)
		So(err, ShouldBeNil)
		So(tmp3.Rows(), ShouldEqual, 1000)
		CheckmodelTableEqual(tmp1.(*model.Table), tmp3.(*model.Table), 1000)
		err = gc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		err = gc.UnSubscribe(req2)
		So(err, ShouldBeNil)
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)

	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_batchSize_throttle(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_batchSize_throttle", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn,
		}
		req1 := &streaming.SubscribeRequest{
			Address:      host1,
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
		waitData(gcConn, receive, 10000)
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
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_batchSize_throttle2(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_batchSize_throttle2", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn,
		}
		filter1, err := gcConn.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:      host1,
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
		waitData(gcConn, receive, 200)
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
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_unsubscribeesubscribe(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_unsubscribeesubscribe", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn,
		}
		filter1, err := gcConn.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:      host1,
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
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_offseteconnect_filter_AllowExistTopic(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_offseteconnect_filter_AllowExistTopic", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		filter1, err := gcConn.RunScript("1..100000")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host1,
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
		go threadWriteData(gcConn, st, 10)
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
		wg.Wait()
		So(reRowNum2.Value(), ShouldBeGreaterThanOrEqualTo, reRowNum1.Value())
		waitData(gcConn, receive, 11000)
		err = gc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_not_contain_handler(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_subscribe_not_contain_handler_1000", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		req1 := &streaming.SubscribeRequest{
			Address:    host1,
			TableName:  st,
			ActionName: "subTrades1",
			Offset:     -1,
			Reconnect:  true,
		}
		err := gc.Subscribe(req1)
		So(err.Error(), ShouldContainSubstring, "if BatchSize is not set, the callback in Handler will be called, so it shouldn't be nil")
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_msgAsTable(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_msgAsTable", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		handler := MessageHandler_table{
			receive: receive,
			conn:    gcConn,
		}
		req1 := &streaming.SubscribeRequest{
			Address:    host1,
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
		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_subscribe_with_StreamDeserializer(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)

	Convey("TestGoroutineClient_subscribe_onehandler_with_StreamDeserializer", t, func() {
		sdhandler, _ := createStreamDeserializer(gcConn, "tonehandler")
		req1 := &streaming.SubscribeRequest{
			Address:    host1,
			TableName:  "tonehandler",
			ActionName: "testStreamDeserializer",
			Offset:     0,
			Handler:    &sdhandler,
			Reconnect:  true,
		}

		targetows := 2000
		err := gc.Subscribe(req1)
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
		ans1, err := gcConn.RunScript("res = select * from res1 order by datetimev,timestampv;ex= select * from table1 order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans1.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}

		ans2, err := gcConn.RunScript("res = select * from res2 order by datetimev,timestampv;ex= select * from table2 order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans2.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}

	})
	Convey("TestGoroutineClient_subscribe_batchHandler_with_StreamDeserializer", t, func() {
		_, sdBatchHandler := createStreamDeserializer(gcConn, "tbatchHandler")
		req1 := &streaming.SubscribeRequest{
			Address:      host1,
			TableName:    "tbatchHandler",
			ActionName:   "testStreamDeserializer",
			Offset:       0,
			BatchHandler: &sdBatchHandler,
			Reconnect:    true,
		}
		req1.SetBatchSize(500)
		targetows := 2000
		err := gc.Subscribe(req1)
		So(err, ShouldBeNil)
		fmt.Println("started subscribe...")
		for {
			time.Sleep(1 * time.Second)
			if sdBatchHandler.msg1_total+sdBatchHandler.msg2_total == targetows {
				break
			}
		}
		err = gc.UnSubscribe(req1)
		So(err, ShouldBeNil)

		res_tab1 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1", "price2"}, sdBatchHandler.res1_data)
		res_tab2 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1"}, sdBatchHandler.res2_data)

		gcConn.Upload(map[string]model.DataForm{"res1": res_tab1, "res2": res_tab2})
		ans1, err := gcConn.RunScript("res = select * from res1 order by datetimev,timestampv;ex= select * from table1 order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans1.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}

		ans2, err := gcConn.RunScript("res = select * from res2 order by datetimev,timestampv;ex= select * from table2 order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans2.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}

	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}

func TestGoroutineClient_unsubscribe_in_doEvent(t *testing.T) {
	var gc = streaming.NewGoroutineClient(setup.IP, setup.SubPort)
	Convey("TestGoroutineClient_unsubscribe_in_doEvent", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn)
		handler := MessageHandler_unsubscribeInDoEvent{
			subType:   "gc",
			subClient: gc,
			subReq: &streaming.SubscribeRequest{
				Address:    host1,
				TableName:  st,
				ActionName: "subTrades1",
				Offset:     0},
			successCount: 0,
		}
		_, err := gcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    host1,
			TableName:  st,
			ActionName: "subTrades1",
			Offset:     0,
			Reconnect:  true,
			Handler:    &handler,
		}
		err = gc.Subscribe(req)
		So(err, ShouldBeNil)

		res_inSub, _ := gcConn.RunScript("getStreamingStat().pubConns")
		// fmt.Println(res_inSub)
		time.Sleep(8 * time.Second)
		res_afterSub, _ := gcConn.RunScript("getStreamingStat().pubConns")
		// fmt.Println(res_afterSub)
		So(res_inSub.(*model.Table).GetColumnByName("tables").String(), ShouldContainSubstring, st)
		So(res_afterSub.(*model.Table).GetColumnByName("tables").String(), ShouldNotContainSubstring, st)
		So(handler.successCount, ShouldEqual, 1)

		ClearStreamTable(host1, st)
		ClearStreamTable(host1, receive)
	})
	gc.Close()
	assert.True(t, gc.IsClosed())
}
