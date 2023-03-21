package test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/example/util"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/streaming"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

var tpc_r = streaming.NewGoroutineClient(setup.IP, setup.Reverse_subPort)
var gcConn_r, _ = api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
var stopLabel_r bool
var wg_r sync.WaitGroup

func CreateStreamingTableforGcTest_r() {
	_, err := gcConn_r.RunScript("login(`admin,`123456);" +
		"try{dropStreamTable('st1')}catch(ex){};" +
		"try{dropStreamTable('st2')}catch(ex){};" +
		"try{dropStreamTable('Trades')}catch(ex){};" +
		"try{dropStreamTable('Receive')}catch(ex){};try{dropStreamTable('filter')}catch(ex){};")
	AssertNil(err)
	_, err = gcConn_r.RunScript("st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
		"share(st1,`Trades)\t\n" + "setStreamTableFilterColumn(objByName(`Trades),`tag)")
	AssertNil(err)
	_, err = gcConn_r.RunScript("st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
		"share(st2, `Receive)\t\n")
	AssertNil(err)
}

func threadWriteData_r() {
	defer wg_r.Done()
	for {
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		AssertNil(err)
		if stopLabel_r {
			break
		}
	}
}

func waitData_r(tableName string, dataRow int) {
	for {
		tmp, err := gcConn_r.RunScript("(exec count(*) from " + tableName + ")[0]")
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

type MessageHandler_r struct{}

func (s *MessageHandler_r) DoEvent(msg streaming.IMessage) {
	val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
	val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
	script := fmt.Sprintf("insert into Receive values(%s,%s,%s)",
		val0, val1, val2)
	_, err := gcConn_r.RunScript(script)
	util.AssertNil(err)
}

func TestGoroutineClient_bachSize_throttle_r(t *testing.T) {
	Convey("test_NewGoroutinePooledClient_batchSize_lt0", t, func() {
		CreateStreamingTableforGcTest_r()
		filter1, err := gcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "action1",
			Offset:     0,
			Reconnect:  true,
			Filter:     filter1.(*model.Vector),
		}
		req.SetBatchSize(-10000).SetThrottle(1)
		err = tpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		err = tpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
	Convey("test_NewGoroutinePooledClient_throttle_less_than_0", t, func() {
		CreateStreamingTableforGcTest_r()
		filter1, err := gcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "action1",
			Offset:     0,
			Reconnect:  true,
			Filter:     filter1.(*model.Vector),
		}
		req.SetBatchSize(10000).SetThrottle(-10)
		err = tpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		err = tpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
	Convey("test_NewGoroutinePooledClient_MessageHandler_throttle_less_than_0", t, func() {
		gcConn_r, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		CreateStreamingTableforGcTest_r()
		filter1, err := gcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "action1",
			Offset:     0,
			Reconnect:  true,
			Filter:     filter1.(*model.Vector),
			Handler:    new(MessageHandler_r),
		}
		req.SetBatchSize(10000).SetThrottle(10)
		err = tpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		err = tpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
	Convey("test_NewGoroutinePooledClient_MessageHandler_batchSize_lt0", t, func() {
		CreateStreamingTableforGcTest_r()
		filter1, err := gcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "action1",
			Offset:     0,
			Reconnect:  true,
			Filter:     filter1.(*model.Vector),
			Handler:    new(MessageHandler_r),
		}
		req.SetBatchSize(-10000).SetThrottle(-10)
		err = tpc_r.Subscribe(req)
		So(tpc_r.IsClosed(), ShouldBeFalse)
		So(err, ShouldBeNil)
		err = tpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
	Convey("test_NewGoroutinePooledClient_MessageHandler_batchSize_Throttle_lt0", t, func() {
		CreateStreamingTableforGcTest_r()
		filter1, err := gcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "action1",
			Offset:     0,
			Reconnect:  true,
			Filter:     filter1.(*model.Vector),
			Handler:    new(MessageHandler_r),
		}
		req.SetBatchSize(-10000).SetThrottle(-5)
		err = tpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		err = tpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
	Convey("test_NewGoroutinePooledClient_batchSize_Throttle_lt0", t, func() {
		CreateStreamingTableforGcTest_r()
		filter1, err := gcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "action1",
			Offset:     -1,
			Reconnect:  true,
			Filter:     filter1.(*model.Vector),
			Handler:    new(MessageHandler_r),
		}
		req.SetBatchSize(-10000).SetThrottle(-5)
		err = tpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		err = tpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
}

func TestGoroutineClient_tableName_offset_r(t *testing.T) {
	Convey("TestGoroutineClient_tableName_offset", t, func() {
		CreateStreamingTableforGcTest_r()
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "action1",
			Offset:     0,
			Reconnect:  false,
			Handler:    new(MessageHandler_r),
		}
		err = tpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		waitData_r("Receive", 2000)
		reTmp, err := gcConn_r.RunScript("Receive")
		So(err, ShouldBeNil)
		exTmp, err := gcConn_r.RunScript("Trades")
		So(err, ShouldBeNil)
		re := reTmp.(*model.Table)
		ex := exTmp.(*model.Table)
		CheckmodelTableEqual(re, ex, 0)
		err = tpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
}

func TestGoroutineClient_tableName_actionName_r(t *testing.T) {
	Convey("TestGoroutineClient_tableName_actionName", t, func() {
		CreateStreamingTableforGcTest_r()
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "subTrades1",
			Offset:     0,
			Reconnect:  false,
			Handler:    new(MessageHandler_r),
		}
		err = tpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		waitData_r("Receive", 4000)
		reTmp, err := gcConn_r.RunScript("Receive")
		So(err, ShouldBeNil)
		exTmp, err := gcConn_r.RunScript("Trades")
		So(err, ShouldBeNil)
		re := reTmp.(*model.Table)
		ex := exTmp.(*model.Table)
		So(re.Rows(), ShouldEqual, 4000)
		CheckmodelTableEqual(re, ex, 0)
		err = tpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
}

func TestGoroutineClient_tableName_handler_offset_reconnect_success_r(t *testing.T) {
	Convey("TestGoroutineClient_tableName_handler_offset_reconnect_success", t, func() {
		CreateStreamingTableforGcTest_r()
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:   setup.Address,
			TableName: "Trades",
			Offset:    -1,
			Reconnect: true,
			Handler:   new(MessageHandler_r),
		}
		err = tpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		wg_r.Add(1)
		go threadWriteData_r()
		time.Sleep(2 * time.Second)
		_, err = gcConn_r.RunScript("stopPublishTable('" + setup.IP + "'," + strconv.Itoa(setup.Port) + ",'Trades')")
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		_, err = gcConn_r.RunScript("stopPublishTable('" + setup.IP + "'," + strconv.Itoa(setup.Port) + ",'Trades')")
		So(err, ShouldBeNil)
		rowNum1, err := gcConn_r.RunScript("(exec count(*) from Receive)[0]")
		So(err, ShouldBeNil)
		reRowNum1 := rowNum1.(*model.Scalar)
		time.Sleep(3 * time.Second)
		rowNum2, err := gcConn_r.RunScript("(exec count(*) from Receive)[0]")
		So(err, ShouldBeNil)
		reRowNum2 := rowNum2.(*model.Scalar)
		stopLabel_r = true
		wg_r.Wait()
		So(reRowNum2.Value(), ShouldBeGreaterThanOrEqualTo, reRowNum1.Value())
		err = tpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_reconnect_r(t *testing.T) {
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_reconnect", t, func() {
		CreateStreamingTableforGcTest_r()
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "subTrades1",
			Reconnect:  true,
			Handler:    new(MessageHandler_r),
		}
		err = tpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		wg_r.Add(1)
		go threadWriteData_r()
		time.Sleep(2 * time.Second)
		_, err = gcConn_r.RunScript("stopPublishTable('" + setup.IP + "'," + strconv.Itoa(setup.Port) + ",'Trades')")
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		_, err = gcConn_r.RunScript("stopPublishTable('" + setup.IP + "'," + strconv.Itoa(setup.Port) + ",'Trades')")
		So(err, ShouldBeNil)
		rowNum1, err := gcConn_r.RunScript("(exec count(*) from Receive)[0]")
		So(err, ShouldBeNil)
		reRowNum1 := rowNum1.(*model.Scalar)
		time.Sleep(3 * time.Second)
		rowNum2, err := gcConn_r.RunScript("(exec count(*) from Receive)[0]")
		So(err, ShouldBeNil)
		reRowNum2 := rowNum2.(*model.Scalar)
		stopLabel_r = true
		wg_r.Wait()
		So(reRowNum2.Value(), ShouldBeGreaterThanOrEqualTo, reRowNum1.Value())
		err = tpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_0_r(t *testing.T) {
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_0", t, func() {
		CreateStreamingTableforGcTest_r()
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "subTrades1",
			Offset:     0,
			Handler:    new(MessageHandler_r),
		}
		err = tpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		waitData_r("Receive", 2000)
		tmp1, err := gcConn_r.RunScript("Receive")
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		tmp2, err := gcConn_r.RunScript("Trades")
		So(err, ShouldBeNil)
		ex := tmp2.(*model.Table)
		CheckmodelTableEqual(re, ex, 0)
		err = tpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_negative_r(t *testing.T) {
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_negative", t, func() {
		CreateStreamingTableforGcTest_r()
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.04T12:23:45+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "subTrades1",
			Offset:     -3,
			Handler:    new(MessageHandler_r),
		}
		err = tpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.01T12:23:45+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.02T12:23:45+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.03T12:23:45+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		waitData_r("Receive", 3000)
		tmp1, err := gcConn_r.RunScript("Receive")
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		tmp2, err := gcConn_r.RunScript("select * from Trades where rowNo(tag)>=1000")
		So(err, ShouldBeNil)
		ex := tmp2.(*model.Table)
		So(re.Rows(), ShouldEqual, 3000)
		CheckmodelTableEqual(re, ex, 0)
		err = tpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_10_r(t *testing.T) {
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_10", t, func() {
		CreateStreamingTableforGcTest_r()
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.04T12:23:45+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "subTradesOffset",
			Offset:     10,
			Handler:    new(MessageHandler_r),
		}
		err = tpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.01T12:23:45+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.02T12:23:45+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.03T12:23:45+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		waitData_r("Receive", 3990)
		tmp1, err := gcConn_r.RunScript("Receive")
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		tmp2, err := gcConn_r.RunScript("select * from Trades where rowNo(tag)>=10")
		So(err, ShouldBeNil)
		ex := tmp2.(*model.Table)
		So(re.Rows(), ShouldEqual, 3990)
		CheckmodelTableEqual(re, ex, 0)
		err = tpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
}

func TestGoroutineClient_subscribe_offset_morethan_tableCount_r(t *testing.T) {
	Convey("TestGoroutineClient_subscribe_offset_morethan_tableCount", t, func() {
		CreateStreamingTableforGcTest_r()
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,2020.01.04T12:23:45+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "subTrades1",
			Offset:     1000,
			Handler:    new(MessageHandler_r),
		}
		err = tpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		time.Sleep(3 * time.Second)
		tmp1, err := gcConn_r.RunScript("Receive")
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		So(re.Rows(), ShouldEqual, 0)
		err = tpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
}

type Handlerx_r struct{}

func (s *Handlerx_r) DoEvent(msg streaming.IMessage) {
	val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
	val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
	script := fmt.Sprintf("insert into filter values(%s,%s,%s)",
		val0, val1, val2)
	_, err := gcConn_r.RunScript(script)
	util.AssertNil(err)
}

func TestGoroutineClient_subscribe_filter_r(t *testing.T) {
	Convey("TestGoroutineClient_subscribe_filter", t, func() {
		CreateStreamingTableforGcTest_r()
		script3 := "try{dropStreamTable('st3')}catch(ex){};try{dropStreamTable('filter')}catch(ex){};go\n" + "st3 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]);" +
			"enableTableShareAndPersistence(table=st3, tableName=`filter, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n"
		_, err := gcConn_r.RunScript(script3)
		AssertNil(err)
		filter1, err := gcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		filter2, err := gcConn_r.RunScript("2001..3000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "subTrades1",
			Offset:     -1,
			Filter:     filter1.(*model.Vector),
			Handler:    new(MessageHandler_r),
		}
		req2 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "subTrades2",
			Offset:     -1,
			Filter:     filter2.(*model.Vector),
			Handler:    new(Handlerx_r),
		}
		err = tpc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		err = tpc_r.Subscribe(req2)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=4000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		waitData_r("Receive", 1000)
		waitData_r("filter", 1000)
		tmp1, err := gcConn_r.RunScript("Receive")
		So(err, ShouldBeNil)
		tmp2, err := gcConn_r.RunScript("Trades")
		So(err, ShouldBeNil)
		tmp3, err := gcConn_r.RunScript("filter")
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("dropStreamTable(`filter)")
		AssertNil(err)
		err = tpc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		err = tpc_r.UnSubscribe(req2)
		So(err, ShouldBeNil)
		re1 := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		re2 := tmp3.(*model.Table)
		So(re1.Rows(), ShouldEqual, 1000)
		So(re2.Rows(), ShouldEqual, 1000)
		CheckmodelTableEqual(re1, ex, 0)
		CheckmodelTableEqual(re2, ex, 2000)
	})
}

func CheckmodelTableEqual_throttle_r(t1 *model.Table, t2 *model.Table, m int, n int) bool {
	for i := 0; i < 1000; i++ {
		for j := 0; j < len(t1.GetColumnNames()); j++ {
			if t1.GetColumnByIndex(j).Get(i+m).Value() != t2.GetColumnByIndex(j).Get(n+i).Value() {
				return false
			}
		}
	}
	return true
}
func TestGoroutineClient_batchSize_throttle_r(t *testing.T) {
	Convey("TestGoroutineClient_batchSize_throttle", t, func() {
		CreateStreamingTableforGcTest_r()
		filter1, err := gcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "subTrades1",
			Offset:     -1,
			Filter:     filter1.(*model.Vector),
			Handler:    new(MessageHandler_r),
			Reconnect:  true,
		}
		req1.SetBatchSize(10000).SetThrottle(5)
		err = tpc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		waitData_r("Receive", 2000)
		tmp1, err := gcConn_r.RunScript("Receive")
		So(err, ShouldBeNil)
		tmp2, err := gcConn_r.RunScript("Trades")
		So(err, ShouldBeNil)
		err = tpc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		re1 := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		So(re1.Rows(), ShouldEqual, 2000)
		fmt.Println(ex.Rows())
		CheckmodelTableEqual_throttle(re1, ex, 0, 0)
		CheckmodelTableEqual_throttle(re1, ex, 1000, 10000)
	})
}

func TestGoroutineClient_batchSize_throttle2_r(t *testing.T) {
	Convey("TestGoroutineClient_batchSize_throttle2", t, func() {
		CreateStreamingTableforGcTest_r()
		filter1, err := gcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "subTrades1",
			Offset:     -1,
			Filter:     filter1.(*model.Vector),
			Handler:    new(MessageHandler_r),
			Reconnect:  true,
		}
		req1.SetBatchSize(10000).SetThrottle(5)
		err = tpc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		waitData_r("Receive", 200)
		tmp1, err := gcConn_r.RunScript("Receive")
		So(err, ShouldBeNil)
		tmp2, err := gcConn_r.RunScript("Trades")
		So(err, ShouldBeNil)
		err = tpc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		re1 := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		So(re1.Rows(), ShouldEqual, 200)
		CheckmodelTableEqual(re1, ex, 0)
	})
}

func TestGoroutineClient_subscribe_unsubscribe_resubscribe_r(t *testing.T) {
	Convey("TestGoroutineClient_subscribe_unsubscribe_resubscribe", t, func() {
		CreateStreamingTableforGcTest_r()
		filter1, err := gcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "subTrades1",
			Offset:     -1,
			Filter:     filter1.(*model.Vector),
			Handler:    new(MessageHandler_r),
			Reconnect:  true,
		}
		req1.SetBatchSize(10000).SetThrottle(5)
		err = tpc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		err = tpc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		err = tpc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		err = tpc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
	})
}

func TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_reconnect_filter_AllowExistTopic_r(t *testing.T) {
	Convey("TestGoroutineClient_subscribe_TableName_ActionName_Handler_offset_reconnect_filter_AllowExistTopic", t, func() {
		CreateStreamingTableforGcTest_r()
		_, err := gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		filter1, err := gcConn_r.RunScript("1..100000")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:     setup.Address,
			TableName:   "Trades",
			ActionName:  "subTrades1",
			Offset:      0,
			Reconnect:   true,
			Filter:      filter1.(*model.Vector),
			Handler:     new(MessageHandler_r),
			AllowExists: true,
		}
		req.SetBatchSize(100).SetThrottle(5)
		err = tpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		wg_r.Add(1)
		go threadWriteData_r()
		time.Sleep(2 * time.Second)
		_, err = gcConn_r.RunScript("stopPublishTable('" + setup.IP + "'," + strconv.Itoa(setup.SubPort) + ",'Trades', 'subTrades1')")
		So(err, ShouldBeNil)
		rowNum1, err := gcConn_r.RunScript("(exec count(*) from Receive)[0]")
		So(err, ShouldBeNil)
		reRowNum1 := rowNum1.(*model.Scalar)
		time.Sleep(3 * time.Second)
		rowNum2, err := gcConn_r.RunScript("(exec count(*) from Receive)[0]")
		So(err, ShouldBeNil)
		reRowNum2 := rowNum2.(*model.Scalar)
		stopLabel_r = true
		wg_r.Wait()
		So(reRowNum2.Value(), ShouldBeGreaterThanOrEqualTo, reRowNum1.Value())
		err = tpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
}

func TestGoroutineClient_subscribe_not_contain_handler_r(t *testing.T) {
	Convey("TestGoroutineClient_subscribe_not_contain_handler_1000", t, func() {
		CreateStreamingTableforGcTest_r()
		req1 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "Trades",
			ActionName: "subTrades1",
			Offset:     -1,
			Reconnect:  true,
		}
		err := tpc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)")
		So(err, ShouldBeNil)
		tmp2, err := gcConn_r.RunScript("Trades")
		So(err, ShouldBeNil)
		err = tpc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		ex := tmp2.(*model.Table)
		So(1000, ShouldEqual, ex.Rows())
	})
}

func TestClear_r(t *testing.T) {
	Convey("test_clear_gc", t, func() {
		So(tpc_r.IsClosed(), ShouldBeFalse)
		tpc_r.Close()
		So(tpc_r.IsClosed(), ShouldBeTrue)
		So(gcConn_r.Close(), ShouldBeNil)
	})
}
