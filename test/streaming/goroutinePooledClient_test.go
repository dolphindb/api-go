package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/example/util"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/streaming"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

var gpc = streaming.NewGoroutinePooledClient(setup.IP, 8696)
var gpcConn, _ = api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)

func CreateStreamingTableforGpcTest() {
	_, err := gpcConn.RunScript("login(`admin,`123456);" +
		"try{dropStreamTable('TradesTable')}catch(ex){};" +
		"try{dropStreamTable('ReceiveTable')}catch(ex){};try{dropStreamTable('filter')}catch(ex){};")
	AssertNil(err)
	_, err = gpcConn.RunScript("st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
		"enableTableShareAndPersistence(table=st1, tableName=`TradesTable, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n" + "setStreamTableFilterColumn(objByName(`TradesTable),`tag)")
	AssertNil(err)
	_, err = gpcConn.RunScript("st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
		"enableTableShareAndPersistence(table=st2, tableName=`ReceiveTable, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n")
	AssertNil(err)
}

func waitDataGpc(tableName string, dataRow int) {
	for {
		tmp, err := gpcConn.RunScript("(exec count(*) from " + tableName + ")[0]")
		AssertNil(err)
		rowNum := tmp.(*model.Scalar)
		fmt.Printf("\nexpectedData is: %v", dataRow)
		fmt.Printf("\nactualData is: %v", rowNum)
		if dataRow == int(rowNum.Value().(int32)) {
			break
		}
		time.Sleep(2 * time.Second)
	}
}

type gpcMessageHandler struct{}

func (s *gpcMessageHandler) DoEvent(msg streaming.IMessage) {
	val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
	val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
	script := fmt.Sprintf("insert into ReceiveTable values(%s,%s,%s)",
		val0, val1, val2)
	_, err := gpcConn.RunScript(script)
	util.AssertNil(err)
}

func TestNewGoroutinePooledClient_subscribe_ex_ubsubscribe(t *testing.T) {
	Convey("TestNewGoroutinePooledClient_subscribe_ex_ubsubscribe", t, func() {
		CreateStreamingTableforGpcTest()
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "TradesTable",
			ActionName: "subTradesTable",
			Handler:    new(gpcMessageHandler),
		}
		req.SetBatchSize(-10000).SetThrottle(1)
		err := gpc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "TradesTable.append!(t)")
		So(err, ShouldBeNil)
		waitDataGpc("ReceiveTable", 1000)
		tmp1, err := gpcConn.RunScript("select * from ReceiveTable order by tag")
		So(err, ShouldBeNil)
		tmp2, err := gpcConn.RunScript("select * from TradesTable order by tag")
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("dropStreamTable('TradesTable');dropStreamTable('ReceiveTable')")
		So(err, ShouldNotBeNil)
		re := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		CheckmodelTableEqual(re, ex, 0)
		err = gpc.UnSubscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("dropStreamTable('TradesTable');dropStreamTable('ReceiveTable')")
		So(err, ShouldBeNil)
	})
}

func TestNewGoroutinePooledClient_subscribe_ex_ActionName(t *testing.T) {
	Convey("TestNewGoroutinePooledClient_subscribe_ex_ActionName", t, func() {
		CreateStreamingTableforGpcTest()
		req := &streaming.SubscribeRequest{
			Address:   setup.Address,
			TableName: "TradesTable",
			Offset:    0,
			Reconnect: true,
			Handler:   new(gpcMessageHandler),
		}
		err := gpc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "TradesTable.append!(t)")
		So(err, ShouldBeNil)
		waitDataGpc("ReceiveTable", 1000)
		err = gpc.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
}

func TestNewGoroutinePooledClient_subscribe_exTableName(t *testing.T) {
	Convey("TestNewGoroutinePooledClient_subscribe_exTableName", t, func() {
		CreateStreamingTableforGpcTest()
		req := &streaming.SubscribeRequest{
			Address:   setup.Address,
			Offset:    0,
			Reconnect: true,
			Handler:   new(gpcMessageHandler),
		}
		err := gpc.Subscribe(req)
		So(err, ShouldNotBeNil)
	})
}
func TestNewGoroutinePooledClient_subscribe_ex_offset(t *testing.T) {
	Convey("TestNewGoroutinePooledClient_subscribe_ex_offset", t, func() {
		CreateStreamingTableforGpcTest()
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "TradesTable",
			ActionName: "subTradesTable",
			Offset:     -2,
			Reconnect:  true,
			Handler:    new(gpcMessageHandler),
		}
		err := gpc.Subscribe(req)
		So(err, ShouldNotBeNil)
	})
}

func TestNewGoroutinePooledClient_subscribe_offset_0(t *testing.T) {
	Convey("TestNewGoroutinePooledClient_subscribe_offset_0", t, func() {
		CreateStreamingTableforGpcTest()
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "TradesTable",
			ActionName: "subTradesTable",
			Offset:     0,
			Reconnect:  true,
			Handler:    new(gpcMessageHandler),
		}
		req.SetBatchSize(-10000).SetThrottle(1)
		err := gpc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "TradesTable.append!(t)")
		So(err, ShouldBeNil)
		waitDataGpc("ReceiveTable", 1000)
		tmp1, err := gpcConn.RunScript("select * from ReceiveTable order by tag")
		So(err, ShouldBeNil)
		tmp2, err := gpcConn.RunScript("select * from TradesTable order by tag")
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		So(re.Rows(), ShouldEqual, 1000)
		So(ex.Rows(), ShouldEqual, 1000)
		CheckmodelTableEqual(re, ex, 0)
		err = gpc.UnSubscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("dropStreamTable('TradesTable');dropStreamTable('ReceiveTable')")
		So(err, ShouldBeNil)
	})
}

func TestNewGoroutinePooledClient_subscribe_offset_negative(t *testing.T) {
	Convey("TestNewGoroutinePooledClient_subscribe_offset_negative", t, func() {
		CreateStreamingTableforGpcTest()
		_, err := gpcConn.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "TradesTable.append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "TradesTable",
			ActionName: "subTradesTable",
			Offset:     -1,
			Reconnect:  true,
			Handler:    new(gpcMessageHandler),
		}
		req.SetBatchSize(-10000).SetThrottle(1)
		err = gpc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "TradesTable.append!(t)")
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "TradesTable.append!(t)")
		So(err, ShouldBeNil)
		waitDataGpc("ReceiveTable", 2000)
		tmp1, err := gpcConn.RunScript("select * from ReceiveTable order by tag, ts, data")
		So(err, ShouldBeNil)
		tmp2, err := gpcConn.RunScript("select * from TradesTable where rowNo(tag)>=100 order by tag, ts, data")
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		So(re.Rows(), ShouldEqual, 2000)
		So(ex.Rows(), ShouldEqual, 2000)
		CheckmodelTableEqual(re, ex, 0)
		err = gpc.UnSubscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("dropStreamTable('TradesTable');dropStreamTable('ReceiveTable')")
		So(err, ShouldBeNil)
	})
}

func TestNewGoroutinePooledClient_subscribe_offset_10(t *testing.T) {
	Convey("TestNewGoroutinePooledClient_subscribe_offset_10", t, func() {
		CreateStreamingTableforGpcTest()
		_, err := gpcConn.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "TradesTable.append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "TradesTable",
			ActionName: "subTradesTable",
			Offset:     10,
			Reconnect:  true,
			Handler:    new(gpcMessageHandler),
		}
		req.SetBatchSize(-10000).SetThrottle(1)
		err = gpc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "TradesTable.append!(t)")
		So(err, ShouldBeNil)
		waitDataGpc("ReceiveTable", 1090)
		tmp1, err := gpcConn.RunScript("select * from ReceiveTable order by tag")
		So(err, ShouldBeNil)
		tmp2, err := gpcConn.RunScript("select * from TradesTable where rowNo(tag)>=10 order by tag")
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		So(re.Rows(), ShouldEqual, 1090)
		So(ex.Rows(), ShouldEqual, 1090)
		err = gpc.UnSubscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("dropStreamTable('TradesTable');dropStreamTable('ReceiveTable')")
		So(err, ShouldBeNil)
	})
}

func TestNewGoroutinePooledClient_subscribe_offset_morethan_rowCount(t *testing.T) {
	Convey("TestNewGoroutinePooledClient_subscribe_offset_morethan_rowCount", t, func() {
		CreateStreamingTableforGpcTest()
		_, err := gpcConn.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "TradesTable.append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "TradesTable",
			ActionName: "subTradesTable",
			Offset:     1000,
			Reconnect:  true,
			Handler:    new(gpcMessageHandler),
		}
		err = gpc.Subscribe(req)
		So(err, ShouldNotBeNil)
	})
}

type Handlegpc struct{}

func (s *Handlegpc) DoEvent(msg streaming.IMessage) {
	val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
	val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
	script := fmt.Sprintf("insert into filter values(%s,%s,%s)",
		val0, val1, val2)
	_, err := gpcConn.RunScript(script)
	util.AssertNil(err)
}

func TestNewGoroutinePooledClient_subscribe_filter(t *testing.T) {
	Convey("TestNewGoroutinePooledClient_subscribe_filter", t, func() {
		CreateStreamingTableforGpcTest()
		script2 := "tmp3 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
			"enableTableShareAndPersistence(table=tmp3, tableName=`filter, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n"
		_, err := gpcConn.RunScript(script2)
		So(err, ShouldBeNil)
		filter1, err := gpcConn.RunScript("1..1000")
		So(err, ShouldBeNil)
		filter2, err := gpcConn.RunScript("2001..3000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "TradesTable",
			ActionName: "subTradesTable1",
			Offset:     0,
			Reconnect:  true,
			Filter:     filter1.(*model.Vector),
			Handler:    new(gpcMessageHandler),
		}
		req2 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "TradesTable",
			ActionName: "subTradesTable2",
			Offset:     0,
			Reconnect:  true,
			Filter:     filter2.(*model.Vector),
			Handler:    new(Handlegpc),
		}
		err = gpc.Subscribe(req1)
		So(err, ShouldBeNil)
		err = gpc.Subscribe(req2)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=4000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "TradesTable.append!(t)")
		So(err, ShouldBeNil)
		waitDataGpc("ReceiveTable", 1000)
		waitDataGpc("filter", 1000)
		tmp1, err := gpcConn.RunScript("select * from ReceiveTable order by tag, ts, data")
		So(err, ShouldBeNil)
		tmp2, err := gpcConn.RunScript("select * from TradesTable order by tag, ts, data")
		So(err, ShouldBeNil)
		tmp3, err := gpcConn.RunScript("select * from filter order by tag, ts, data")
		So(err, ShouldBeNil)
		re1 := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		re2 := tmp3.(*model.Table)
		So(re1.Rows(), ShouldEqual, 1000)
		So(re2.Rows(), ShouldEqual, 1000)
		CheckmodelTableEqual(re1, ex, 0)
		CheckmodelTableEqual(re2, ex, 2000)
		err = gpc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		err = gpc.UnSubscribe(req2)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("dropStreamTable('TradesTable');dropStreamTable('ReceiveTable');dropStreamTable('filter')")
		So(err, ShouldBeNil)
	})
}

func TestNewGoroutinePooledClient_batchSize_throttle(t *testing.T) {
	Convey("TestNewGoroutinePooledClient_batchSize_throttle", t, func() {
		CreateStreamingTableforGpcTest()
		filter1, err := gpcConn.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "TradesTable",
			ActionName: "subTradesTable1",
			Offset:     -1,
			Filter:     filter1.(*model.Vector),
			Handler:    new(gpcMessageHandler),
			Reconnect:  true,
		}
		req1.SetBatchSize(10000).SetThrottle(5)
		err = gpc.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "TradesTable.append!(t)")
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "TradesTable.append!(t)")
		So(err, ShouldBeNil)
		waitDataGpc("ReceiveTable", 2000)
		tmp1, err := gpcConn.RunScript("select * from ReceiveTable order by tag")
		So(err, ShouldBeNil)
		tmp2, err := gpcConn.RunScript("select * from TradesTable order by tag")
		So(err, ShouldBeNil)
		err = gpc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		re1 := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		So(re1.Rows(), ShouldEqual, 2000)
		So(ex.Rows(), ShouldEqual, 20000)
		_, err = gpcConn.RunScript("dropStreamTable('TradesTable');dropStreamTable('ReceiveTable')")
		So(err, ShouldBeNil)
	})
}

func TestNewGoroutinePooledClient_batchSize_throttle2(t *testing.T) {
	Convey("TestNewGoroutinePooledClient_batchSize_throttle2", t, func() {
		CreateStreamingTableforGpcTest()
		filter1, err := gpcConn.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "TradesTable",
			ActionName: "subTradesTable1",
			Offset:     -1,
			Filter:     filter1.(*model.Vector),
			Handler:    new(gpcMessageHandler),
			Reconnect:  true,
		}
		req1.SetBatchSize(10000).SetThrottle(5)
		err = gpc.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "TradesTable.append!(t)")
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "TradesTable.append!(t)")
		So(err, ShouldBeNil)
		waitDataGpc("ReceiveTable", 200)
		tmp1, err := gpcConn.RunScript("select * from ReceiveTable order by tag,ts,data")
		So(err, ShouldBeNil)
		tmp2, err := gpcConn.RunScript("select * from TradesTable order by tag,ts,data")
		So(err, ShouldBeNil)
		err = gpc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		re1 := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		So(re1.Rows(), ShouldEqual, 200)
		CheckmodelTableEqual(re1, ex, 0)
		_, err = gpcConn.RunScript("dropStreamTable('TradesTable');dropStreamTable('ReceiveTable')")
		So(err, ShouldBeNil)
	})
}

func TestNewGoroutinePooledClient_subscribe_unsubscribe_resubscribe(t *testing.T) {
	Convey("TestNewGoroutinePooledClient_subscribe_unsubscribe_resubscribe", t, func() {
		CreateStreamingTableforGpcTest()
		filter1, err := gpcConn.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "TradesTable",
			ActionName: "subTradesTable1",
			Offset:     -1,
			Filter:     filter1.(*model.Vector),
			Handler:    new(gpcMessageHandler),
			Reconnect:  true,
		}
		req1.SetBatchSize(10000).SetThrottle(5)
		err = gpc.Subscribe(req1)
		So(err, ShouldBeNil)
		err = gpc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		err = gpc.Subscribe(req1)
		So(err, ShouldBeNil)
		err = gpc.UnSubscribe(req1)
		So(err, ShouldBeNil)
	})
}

func TestClearGpc(t *testing.T) {
	Convey("test_clear_gpc", t, func() {
		So(gpc.IsClosed(), ShouldBeFalse)
		gpc.Close()
		So(gpc.IsClosed(), ShouldBeTrue)
		So(gpcConn.Close(), ShouldBeNil)
	})
}
