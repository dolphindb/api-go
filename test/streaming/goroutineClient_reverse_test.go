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

var host = getRandomClusterAddress()
var gcConn_r, _ = api.NewSimpleDolphinDBClient(context.TODO(), host, setup.UserName, setup.Password)

func TestGoroutineClient_bachSize_throttle_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("test_NewGoroutinePooledClient_batchSize_lt0", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageHandler{
			receive: receive,
			conn:    gcConn_r,
		}
		req := &streaming.SubscribeRequest{
			Address:    host,
			TableName:  st,
			ActionName: "action1",
			Offset:     0,
			Reconnect:  true,
			Handler:    &handler,
		}
		req.SetBatchSize(-1)
		err := gc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		for {
			res, err := gcConn_r.RunScript("exec * from " + receive)
			So(err, ShouldBeNil)
			if res.Rows() == 1000 {
				break
			}
		}

		err = gc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	Convey("test_NewGoroutinePooledClient_throttle_less_than_0", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn_r,
		}
		req := &streaming.SubscribeRequest{
			Address:      host,
			TableName:    st,
			ActionName:   "action1",
			Offset:       0,
			Reconnect:    true,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000).SetThrottle(-2)
		err := gc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		for {
			res, err := gcConn_r.RunScript("exec * from " + receive)
			So(err, ShouldBeNil)
			if res.Rows() == 1000 {
				break
			}
		}
		err = gc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_tableName_offset_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("TestGoroutineClient_tableName_offset", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn_r,
		}
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host,
			TableName:    st,
			ActionName:   "action1",
			Offset:       0,
			Reconnect:    false,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(gcConn_r, receive, 2000)
		reTmp, err := gcConn_r.RunScript(receive)
		So(err, ShouldBeNil)
		exTmp, err := gcConn_r.RunScript(st)
		So(err, ShouldBeNil)
		re := reTmp.(*model.Table)
		ex := exTmp.(*model.Table)
		CheckmodelTableEqual(re, ex, 0)
		err = gc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_tableName_actionName_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("TestGoroutineClient_tableName_actionName", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn_r,
		}
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host,
			TableName:    st,
			ActionName:   "subTrades1",
			Offset:       0,
			Reconnect:    false,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(gcConn_r, receive, 4000)
		reTmp, err := gcConn_r.RunScript(receive)
		So(err, ShouldBeNil)
		exTmp, err := gcConn_r.RunScript(st)
		So(err, ShouldBeNil)
		re := reTmp.(*model.Table)
		ex := exTmp.(*model.Table)
		So(re.Rows(), ShouldEqual, 4000)
		CheckmodelTableEqual(re, ex, 0)
		err = gc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_tableName_handler_offset_reconnect_success_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("TestGoroutineClient_tableName_handler_offset_reconnect_success_r", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageHandler{
			receive: receive,
			conn:    gcConn_r,
		}
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:   host,
			TableName: st,
			Offset:    0,
			Reconnect: true,
			Handler:   &handler,
		}
		err = gc_r.Subscribe(req)
		So(err, ShouldBeNil)

		_, err = gcConn_r.RunScript("n=500;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		_, err = gcConn_r.RunScript("stopPublishTable('" + setup.IP + "'," + strings.Split(host, ":")[1] + ",'" + st + "')")
		So(err, ShouldBeNil)

		_, err = gcConn_r.RunScript("n=500;t=table(1..n+500 as tag,now()+1..n+500 as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		_, err = gcConn_r.RunScript("stopPublishTable('" + setup.IP + "'," + strings.Split(host, ":")[1] + ",'" + st + "')")
		So(err, ShouldBeNil)

		time.Sleep(10 * time.Second)
		res, _ := gcConn_r.RunScript("res = select * from " + receive + " order by tag;ex = select * from " + st + " order by tag;each(eqObj, ex.values(), res.values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}

		err = gc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_reconnect_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_reconnect", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn_r,
		}
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host,
			TableName:    st,
			ActionName:   "subTrades1",
			Reconnect:    true,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gc_r.Subscribe(req)
		So(err, ShouldBeNil)
		wg.Add(1)
		go threadWriteData(gcConn_r, st, 10)
		time.Sleep(2 * time.Second)
		_, err = gcConn_r.RunScript("stopPublishTable('" + setup.IP + "'," + strings.Split(host, ":")[1] + ",'" + st + "')")
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		_, err = gcConn_r.RunScript("stopPublishTable('" + setup.IP + "'," + strings.Split(host, ":")[1] + ",'" + st + "')")
		So(err, ShouldBeNil)
		rowNum1, err := gcConn_r.RunScript("(exec count(*) from " + receive + ")[0]")
		So(err, ShouldBeNil)
		reRowNum1 := rowNum1.(*model.Scalar)
		time.Sleep(3 * time.Second)
		rowNum2, err := gcConn_r.RunScript("(exec count(*) from " + receive + ")[0]")
		So(err, ShouldBeNil)
		reRowNum2 := rowNum2.(*model.Scalar)
		wg.Wait()
		So(reRowNum2.Value(), ShouldBeGreaterThanOrEqualTo, reRowNum1.Value())
		waitData(gcConn_r, receive, 11000)
		err = gc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_0_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_0", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn_r,
		}
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host,
			TableName:    st,
			ActionName:   "subTrades1",
			Offset:       0,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(gcConn_r, receive, 2000)
		tmp1, err := gcConn_r.RunScript(receive)
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		tmp2, err := gcConn_r.RunScript(st)
		So(err, ShouldBeNil)
		ex := tmp2.(*model.Table)
		CheckmodelTableEqual(re, ex, 0)
		err = gc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_negative_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_negative", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn_r,
		}
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.04T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host,
			TableName:    st,
			ActionName:   "subTrades1",
			Offset:       -3,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.01T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.02T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.03T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(gcConn_r, receive, 3000)
		tmp1, err := gcConn_r.RunScript(receive)
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		tmp2, err := gcConn_r.RunScript("select * from " + st + " where rowNo(tag)>=1000")
		So(err, ShouldBeNil)
		ex := tmp2.(*model.Table)
		So(re.Rows(), ShouldEqual, 3000)
		CheckmodelTableEqual(re, ex, 0)
		err = gc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_10_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_10", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn_r,
		}
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.04T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host,
			TableName:    st,
			ActionName:   "subTradesOffset",
			Offset:       10,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.01T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.02T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.03T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(gcConn_r, receive, 3990)
		tmp1, err := gcConn_r.RunScript(receive)
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		tmp2, err := gcConn_r.RunScript("select * from " + st + " where rowNo(tag)>=10")
		So(err, ShouldBeNil)
		ex := tmp2.(*model.Table)
		So(re.Rows(), ShouldEqual, 3990)
		CheckmodelTableEqual(re, ex, 0)
		err = gc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_subscribe_offset_morethan_tableCount_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("TestGoroutineClient_subscribe_offset_morethan_tableCount", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn_r,
		}
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.04T12:23:45+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host,
			TableName:    st,
			ActionName:   "subTrades1",
			Offset:       1000,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gc_r.Subscribe(req)
		So(err, ShouldBeNil)
		time.Sleep(3 * time.Second)
		tmp1, err := gcConn_r.RunScript(receive)
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		So(re.Rows(), ShouldEqual, 0)
		err = gc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_subscribe_filter_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("TestGoroutineClient_subscribe_filter", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageHandler{
			receive: receive,
			conn:    gcConn_r,
		}
		filter1, err := gcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		filter2, err := gcConn_r.RunScript("2001..3000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:    host,
			TableName:  st,
			ActionName: "subTrades1",
			Offset:     0,
			Filter:     filter1.(*model.Vector),
			Handler:    &handler,
		}
		req2 := &streaming.SubscribeRequest{
			Address:    host,
			TableName:  st,
			ActionName: "subTrades2",
			Offset:     0,
			Filter:     filter2.(*model.Vector),
			Handler:    &handler,
		}
		err = gc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=4000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		tmp1, err := gcConn_r.RunScript(receive)
		So(err, ShouldBeNil)
		waitData(gcConn_r, receive, 1000)
		tmp3, err := gcConn_r.RunScript(receive)
		So(err, ShouldBeNil)
		So(tmp3.Rows(), ShouldEqual, 1000)
		CheckmodelTableEqual(tmp1.(*model.Table), tmp3.(*model.Table), 1000)

		err = gc_r.Subscribe(req2)
		So(err, ShouldBeNil)
		waitData(gcConn_r, receive, 1000)
		tmp3, err = gcConn_r.RunScript(receive)
		So(err, ShouldBeNil)
		So(tmp3.Rows(), ShouldEqual, 1000)
		CheckmodelTableEqual(tmp1.(*model.Table), tmp3.(*model.Table), 1000)
		err = gc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		err = gc_r.UnSubscribe(req2)
		So(err, ShouldBeNil)
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)

	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_batchSize_throttle_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("TestGoroutineClient_batchSize_throttle", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn_r,
		}
		req1 := &streaming.SubscribeRequest{
			Address:      host,
			TableName:    st,
			ActionName:   "subTrades1",
			Offset:       -1,
			BatchHandler: &handler,
			Reconnect:    true,
		}
		req1.SetBatchSize(10000).SetThrottle(5)
		err := gc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(gcConn_r, receive, 10000)
		tmp1, err := gcConn_r.RunScript(receive)
		So(err, ShouldBeNil)
		err = gc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		re1 := tmp1.(*model.Table)
		So(re1.Rows(), ShouldEqual, 10000)
		res, _ := gcConn_r.RunScript("each(eqObj, " + st + ".values(), " + receive + ".values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_batchSize_throttle2_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("TestGoroutineClient_batchSize_throttle2", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn_r,
		}
		filter1, err := gcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:      host,
			TableName:    st,
			ActionName:   "subTrades1",
			Offset:       -1,
			Filter:       filter1.(*model.Vector),
			BatchHandler: &handler,
			Reconnect:    true,
		}
		req1.SetBatchSize(10000).SetThrottle(5)
		err = gc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(gcConn_r, receive, 200)
		tmp1, err := gcConn_r.RunScript(receive)
		So(err, ShouldBeNil)
		tmp2, err := gcConn_r.RunScript(st)
		So(err, ShouldBeNil)
		err = gc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		re1 := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		So(re1.Rows(), ShouldEqual, 200)
		CheckmodelTableEqual(re1, ex, 0)
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_subscribe_unsubscribeesubscribe_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("TestGoroutineClient_subscribe_unsubscribeesubscribe", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn_r,
		}
		filter1, err := gcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:      host,
			TableName:    st,
			ActionName:   "subTrades1",
			Offset:       -1,
			Filter:       filter1.(*model.Vector),
			BatchHandler: &handler,
			Reconnect:    true,
		}
		req1.SetBatchSize(10000).SetThrottle(5)
		err = gc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		err = gc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		err = gc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		err = gc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_offseteconnect_filter_AllowExistTopic_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_offseteconnect_filter_AllowExistTopic", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gcConn_r,
		}
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		filter1, err := gcConn_r.RunScript("1..100000")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host,
			TableName:    st,
			ActionName:   "subTrades1",
			Offset:       0,
			Reconnect:    true,
			Filter:       filter1.(*model.Vector),
			BatchHandler: &handler,
			AllowExists:  true,
		}
		req.SetBatchSize(100).SetThrottle(5)
		err = gc_r.Subscribe(req)
		So(err, ShouldBeNil)
		wg.Add(1)
		go threadWriteData(gcConn_r, st, 10)
		time.Sleep(2 * time.Second)
		_, err = gcConn_r.RunScript("stopPublishTable('" + setup.IP + "'," + strconv.Itoa(setup.Reverse_subPort) + ",'" + st + "', 'subTrades1')")
		So(err, ShouldBeNil)
		rowNum1, err := gcConn_r.RunScript("(exec count(*) from " + receive + ")[0]")
		So(err, ShouldBeNil)
		reRowNum1 := rowNum1.(*model.Scalar)
		time.Sleep(3 * time.Second)
		rowNum2, err := gcConn_r.RunScript("(exec count(*) from " + receive + ")[0]")
		So(err, ShouldBeNil)
		reRowNum2 := rowNum2.(*model.Scalar)
		wg.Wait()
		So(reRowNum2.Value(), ShouldBeGreaterThanOrEqualTo, reRowNum1.Value())
		waitData(gcConn_r, receive, 11000)
		err = gc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_subscribe_not_contain_handler_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("TestGoroutineClient_subscribe_not_contain_handler_1000", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		req1 := &streaming.SubscribeRequest{
			Address:    host,
			TableName:  st,
			ActionName: "subTrades1",
			Offset:     -1,
			Reconnect:  true,
		}
		err := gc_r.Subscribe(req1)
		So(err.Error(), ShouldContainSubstring, "if BatchSize is not set, the callback in Handler will be called, so it shouldn't be nil")
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_msgAsTable_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("TestGoroutineClient_msgAsTable", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageHandler_table{
			receive: receive,
			conn:    gcConn_r,
		}
		req1 := &streaming.SubscribeRequest{
			Address:    host,
			TableName:  st,
			ActionName: "subTrades1",
			Offset:     0,
			Handler:    &handler,
			Reconnect:  true,
			MsgAsTable: true,
		}
		req1.SetBatchSize(1000)
		err := gc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		for {
			res, err := gcConn_r.RunScript("exec * from " + receive)
			So(err, ShouldBeNil)
			if res.Rows() == 1000 {
				break
			}
		}
		err = gc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_subscribe_with_StreamDeserializer_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)

	Convey("TestGoroutineClient_subscribe_onehandler_with_StreamDeserializer", t, func() {
		sdhandler, _ := createStreamDeserializer(gcConn_r, "SDoutTables_gc_r")
		req1 := &streaming.SubscribeRequest{
			Address:    host,
			TableName:  "SDoutTables_gc_r",
			ActionName: "testStreamDeserializer",
			Offset:     0,
			Handler:    &sdhandler,
			Reconnect:  true,
		}

		targetows := 2000
		err := gc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		fmt.Println("started subscribe...")
		for {
			time.Sleep(1 * time.Second)
			if sdhandler.msg1_total+sdhandler.msg2_total == targetows {
				break
			}
		}
		err = gc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)

		res_tab1 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1", "price2"}, sdhandler.res1_data)
		res_tab2 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1"}, sdhandler.res2_data)

		gcConn_r.Upload(map[string]model.DataForm{"res1": res_tab1, "res2": res_tab2})
		ans1, err := gcConn_r.RunScript("res = select * from res1 order by datetimev,timestampv;ex= select * from table1 order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans1.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}

		ans2, err := gcConn_r.RunScript("res = select * from res2 order by datetimev,timestampv;ex= select * from table2 order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans2.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}
		_, err = gcConn_r.RunScript(
			"try{ dropStreamTable(`SDoutTables_gc_r);}catch(ex){};" +
				"try{ dropStreamTable(`st2);}catch(ex){};" +
				"try{ undef(`table1, SHARED);}catch(ex){};" +
				"try{ undef(`table2, SHARED);}catch(ex){};go")
		So(err, ShouldBeNil)

	})
	Convey("TestGoroutineClient_subscribe_batchHandler_with_StreamDeserializer", t, func() {
		_, sdBatchHandler := createStreamDeserializer(gcConn_r, "SDoutTables_gc_r")
		req1 := &streaming.SubscribeRequest{
			Address:      host,
			TableName:    "SDoutTables_gc_r",
			ActionName:   "testStreamDeserializer",
			Offset:       0,
			BatchHandler: &sdBatchHandler,
			Reconnect:    true,
		}
		req1.SetBatchSize(500)
		targetows := 2000
		err := gc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		fmt.Println("started subscribe...")
		for {
			time.Sleep(1 * time.Second)
			if sdBatchHandler.msg1_total+sdBatchHandler.msg2_total == targetows {
				break
			}
		}
		err = gc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)

		res_tab1 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1", "price2"}, sdBatchHandler.res1_data)
		res_tab2 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1"}, sdBatchHandler.res2_data)

		gcConn_r.Upload(map[string]model.DataForm{"res1": res_tab1, "res2": res_tab2})
		ans1, err := gcConn_r.RunScript("res = select * from res1 order by datetimev,timestampv;ex= select * from table1 order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans1.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}

		ans2, err := gcConn_r.RunScript("res = select * from res2 order by datetimev,timestampv;ex= select * from table2 order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans2.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}
		_, err = gcConn_r.RunScript(
			"try{ dropStreamTable(`SDoutTables_gc_r);}catch(ex){};" +
				"try{ dropStreamTable(`st2);}catch(ex){};" +
				"try{ undef(`table1, SHARED);}catch(ex){};" +
				"try{ undef(`table2, SHARED);}catch(ex){};go")
		So(err, ShouldBeNil)

	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}

func TestGoroutineClient_unsubscribe_in_doEvent_r(t *testing.T) {
	var gc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
	Convey("TestGoroutineClient_unsubscribe_in_doEvent", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gcConn_r)
		handler := MessageHandler_unsubscribeInDoEvent{
			subType:   "gc",
			subClient: gc_r,
			subReq: &streaming.SubscribeRequest{
				Address:    host,
				TableName:  st,
				ActionName: "subTrades1",
				Offset:     0},
			successCount: 0,
		}
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    host,
			TableName:  st,
			ActionName: "subTrades1",
			Offset:     0,
			Reconnect:  true,
			Handler:    &handler,
		}
		err = gc_r.Subscribe(req)
		So(err, ShouldBeNil)

		res_inSub, _ := gcConn_r.RunScript("getStreamingStat().pubConns")
		// fmt.Println(res_inSub)
		time.Sleep(8 * time.Second)
		res_afterSub, _ := gcConn_r.RunScript("getStreamingStat().pubConns")
		// fmt.Println(res_afterSub)
		So(res_inSub.(*model.Table).GetColumnByName("tables").String(), ShouldContainSubstring, st)
		So(res_afterSub.(*model.Table).GetColumnByName("tables").String(), ShouldNotContainSubstring, st)
		So(handler.successCount, ShouldEqual, 1)

		ClearStreamTable(host, st)
		ClearStreamTable(host, receive)
	})
	gc_r.Close()
	assert.True(t, gc_r.IsClosed())
}
