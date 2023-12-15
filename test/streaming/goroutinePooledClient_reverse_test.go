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

var gpcConn_r, _ = api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)

func TestNewGoroutinePooledClient_subscribe_ex_ubsubscribe_r(t *testing.T) {
	var gpc_r = streaming.NewGoroutinePooledClient(setup.IP, setup.Reverse_subPort)
	Convey("TestNewGoroutinePooledClient_subscribe_ex_ubsubscribe", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler_r{
			receive: receive,
		}

		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTradesTable",
			BatchHandler: &handler,
		}
		req.SetBatchSize(2).SetThrottle(1)
		err := gpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData_r(receive, 1000)
		tmp1, err := gpcConn_r.RunScript("select * from " + receive + " order by tag")
		So(err, ShouldBeNil)
		tmp2, err := gpcConn_r.RunScript("select * from " + st + " order by tag")
		So(err, ShouldBeNil)
		_, err = gpcConn_r.RunScript("dropStreamTable('" + st + "');dropStreamTable('" + receive + "')")
		So(err, ShouldNotBeNil)
		re := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		CheckmodelTableEqual(re, ex, 0)
		err = gpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gpc_r.Close()
	assert.True(t, gpc_r.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_ex_ActionName_r(t *testing.T) {
	var gpc_r = streaming.NewGoroutinePooledClient(setup.IP, setup.Reverse_subPort)
	Convey("TestNewGoroutinePooledClient_subscribe_ex_ActionName", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler_r{
			receive: receive,
		}
		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			Offset:       0,
			Reconnect:    true,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err := gpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData_r(receive, 1000)
		err = gpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gpc_r.Close()
	assert.True(t, gpc_r.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_exTableName_r(t *testing.T) {
	var gpc_r = streaming.NewGoroutinePooledClient(setup.IP, setup.Reverse_subPort)
	Convey("TestNewGoroutinePooledClient_subscribe_exTableName", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler_r{
			receive: receive,
		}
		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			Offset:       0,
			Reconnect:    true,
			BatchHandler: &handler,
		}
		err := gpc_r.Subscribe(req)
		So(err, ShouldNotBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gpc_r.Close()
	assert.True(t, gpc_r.IsClosed())
}
func TestNewGoroutinePooledClient_subscribe_ex_offset_r(t *testing.T) {
	var gpc_r = streaming.NewGoroutinePooledClient(setup.IP, setup.Reverse_subPort)
	Convey("TestNewGoroutinePooledClient_subscribe_ex_offset", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler_r{
			receive: receive,
		}
		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTradesTable",
			Offset:       -2,
			Reconnect:    true,
			BatchHandler: &handler,
		}
		err := gpc_r.Subscribe(req)
		So(err, ShouldNotBeNil)
		err = gpc_r.UnSubscribe(req)
		AssertNil(err)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gpc_r.Close()
	assert.True(t, gpc_r.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_offset_0_r(t *testing.T) {
	var gpc_r = streaming.NewGoroutinePooledClient(setup.IP, setup.Reverse_subPort)
	Convey("TestNewGoroutinePooledClient_subscribe_offset_0", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler_r{
			receive: receive,
		}
		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTradesTable",
			Offset:       0,
			Reconnect:    true,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err := gpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData_r(receive, 1000)

		res, _ := gpcConn_r.RunScript("res = select * from " + receive + " order by tag;ex = select * from " + st + " order by tag;each(eqObj, ex.values(), res.values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}
		err = gpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gpc_r.Close()
	assert.True(t, gpc_r.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_offset_negative_r(t *testing.T) {
	var gpc_r = streaming.NewGoroutinePooledClient(setup.IP, setup.Reverse_subPort)
	Convey("TestNewGoroutinePooledClient_subscribe_offset_negative", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler_r{
			receive: receive,
		}
		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTradesTable",
			Offset:       -1,
			Reconnect:    true,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		_, err := gpcConn_r.RunScript("n=1000;t1=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t1)")
		So(err, ShouldBeNil)

		err = gpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn_r.RunScript("n=2000;t2=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t2)")
		So(err, ShouldBeNil)
		waitData_r(receive, 2000)
		res, err := gpcConn_r.RunScript("res = select * from " + receive + " order by tag;ex = select * from t2 order by tag;each(eqObj, ex.values(), res.values())")
		AssertNil(err)
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}
		err = gpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gpc_r.Close()
	assert.True(t, gpc_r.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_offset_10_r(t *testing.T) {
	var gpc_r = streaming.NewGoroutinePooledClient(setup.IP, setup.Reverse_subPort)
	Convey("TestNewGoroutinePooledClient_subscribe_offset_10", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler_r{
			receive: receive,
		}
		_, err := gpcConn_r.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTradesTable",
			Offset:       10,
			Reconnect:    true,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gpc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData_r(receive, 1090)
		tmp1, err := gpcConn_r.RunScript("select * from " + receive + " order by tag")
		So(err, ShouldBeNil)
		tmp2, err := gpcConn_r.RunScript("select * from " + st + " where rowNo(tag)>=10 order by tag")
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		So(re.Rows(), ShouldEqual, 1090)
		So(ex.Rows(), ShouldEqual, 1090)
		err = gpc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gpc_r.Close()
	assert.True(t, gpc_r.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_offset_morethan_rowCount_r(t *testing.T) {
	var gpc_r = streaming.NewGoroutinePooledClient(setup.IP, setup.Reverse_subPort)
	Convey("TestNewGoroutinePooledClient_subscribe_offset_morethan_rowCount", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler_r{
			receive: receive,
		}
		_, err := gpcConn_r.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTradesTable",
			Offset:       1000,
			Reconnect:    true,
			BatchHandler: &handler,
		}
		err = gpc_r.Subscribe(req)
		So(err, ShouldNotBeNil)
		err = gpc_r.UnSubscribe(req)
		AssertNil(err)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gpc_r.Close()
	assert.True(t, gpc_r.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_filter_r(t *testing.T) {
	var gpc_r = streaming.NewGoroutinePooledClient(setup.IP, setup.Reverse_subPort)
	Convey("TestNewGoroutinePooledClient_subscribe_filter", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageHandler_r{
			receive: receive,
		}
		script2 := "try{dropStreamTable('st3')}catch(ex){};try{dropStreamTable('" + receive + "')}catch(ex){};go;tmp3 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]);" +
			"enableTableShareAndPersistence(table=tmp3, tableName=`" + receive + ", asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n"
		_, err := gpcConn_r.RunScript(script2)
		So(err, ShouldBeNil)
		filter1, err := gpcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		filter2, err := gpcConn_r.RunScript("2001..3000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  st,
			ActionName: "subTradesTable1",
			Offset:     0,
			Reconnect:  true,
			Filter:     filter1.(*model.Vector),
			Handler:    &handler,
		}
		req2 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  st,
			ActionName: "subTradesTable2",
			Offset:     0,
			Reconnect:  true,
			Filter:     filter2.(*model.Vector),
			Handler:    &handler,
		}
		err = gpc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gpcConn_r.RunScript("n=4000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		tmp1, err := gpcConn_r.RunScript("select * from " + receive + " order by tag, ts, data")
		So(err, ShouldBeNil)
		waitData_r(receive, 1000)
		tmp3, err := gpcConn_r.RunScript("select * from " + receive + " order by tag, ts, data")
		So(err, ShouldBeNil)
		So(tmp3.Rows(), ShouldEqual, 1000)
		CheckmodelTableEqual(tmp1.(*model.Table), tmp3.(*model.Table), 1000)

		err = gpc_r.Subscribe(req2)
		So(err, ShouldBeNil)
		waitData_r(receive, 1000)
		tmp3, err = gpcConn_r.RunScript("select * from " + receive + " order by tag, ts, data")
		So(err, ShouldBeNil)
		So(tmp3.Rows(), ShouldEqual, 1000)
		CheckmodelTableEqual(tmp1.(*model.Table), tmp3.(*model.Table), 2000)
		err = gpc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		err = gpc_r.UnSubscribe(req2)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gpc_r.Close()
	assert.True(t, gpc_r.IsClosed())
}

func TestNewGoroutinePooledClient_batchSize_throttle_r(t *testing.T) {
	var gpc_r = streaming.NewGoroutinePooledClient(setup.IP, setup.Reverse_subPort)
	Convey("TestNewGoroutinePooledClient_batchSize_throttle", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler_r{
			receive: receive,
		}
		filter1, err := gpcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTradesTable1",
			Offset:       -1,
			Filter:       filter1.(*model.Vector),
			BatchHandler: &handler,
			Reconnect:    true,
		}
		req1.SetBatchSize(1000).SetThrottle(5)
		err = gpc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gpcConn_r.RunScript("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData_r(receive, 1000)
		res, err := gpcConn_r.RunScript("res = select * from " + receive + " order by tag;ex = select * from " + st + " where tag between 1:1000 order by tag;each(eqObj, ex.values(), res.values())")
		AssertNil(err)
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}
		err = gpc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)

	})
	gpc_r.Close()
	assert.True(t, gpc_r.IsClosed())
}

func TestNewGoroutinePooledClient_tableName_handler_offset_reconnect_success_r(t *testing.T) {
	var pc_r = streaming.NewGoroutinePooledClient(setup.IP, setup.Reverse_subPort)
	Convey("TestNewGoroutinePooledClient_tableName_handler_offset_reconnect_success", t, func() {
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
		err = pc_r.Subscribe(req)
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

		err = pc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	pc_r.Close()
	assert.True(t, pc_r.IsClosed())
}

func TestNewGoroutinePooledClient_batchSize_throttle2_r(t *testing.T) {
	var gpc_r = streaming.NewGoroutinePooledClient(setup.IP, setup.Reverse_subPort)
	Convey("TestNewGoroutinePooledClient_batchSize_throttle2", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler_r{
			receive: receive,
		}
		filter1, err := gpcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTradesTable1",
			Offset:       -1,
			Filter:       filter1.(*model.Vector),
			BatchHandler: &handler,
			Reconnect:    true,
		}
		req1.SetBatchSize(10000).SetThrottle(5)
		err = gpc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gpcConn_r.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		_, err = gpcConn_r.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData_r(receive, 200)
		tmp1, err := gpcConn_r.RunScript("select * from " + receive + " order by tag,ts,data")
		So(err, ShouldBeNil)
		tmp2, err := gpcConn_r.RunScript("select * from " + st + " order by tag,ts,data")
		So(err, ShouldBeNil)
		err = gpc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		re1 := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		So(re1.Rows(), ShouldEqual, 200)
		CheckmodelTableEqual(re1, ex, 0)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gpc_r.Close()
	assert.True(t, gpc_r.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_unsubscribe_resubscribe_r(t *testing.T) {
	var gpc_r = streaming.NewGoroutinePooledClient(setup.IP, setup.Reverse_subPort)
	Convey("TestNewGoroutinePooledClient_subscribe_unsubscribe_resubscribe", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageBatchHandler_r{
			receive: receive,
		}
		filter1, err := gpcConn_r.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    st,
			ActionName:   "subTradesTable1",
			Offset:       -1,
			Filter:       filter1.(*model.Vector),
			BatchHandler: &handler,
			Reconnect:    true,
		}
		req1.SetBatchSize(10000).SetThrottle(5)
		err = gpc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		err = gpc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		err = gpc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		err = gpc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gpc_r.Close()
	assert.True(t, gpc_r.IsClosed())
}

func TestNewGoroutinePooledClient_msgAsTable_r(t *testing.T) {
	var gpc_r = streaming.NewGoroutinePooledClient(setup.IP, setup.Reverse_subPort)
	Convey("TestNewGoroutinePooledClient_msgAsTable", t, func() {
		st, receive := CreateStreamingTableWithRandomName()
		handler := MessageHandler_table_r{
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
		err := gpc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gpcConn_r.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		for {
			res, err := gpcConn_r.RunScript("exec * from " + receive)
			So(err, ShouldBeNil)
			if res.Rows() == 1000 {
				break
			}
		}
		err = gpc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		ClearStreamTable(st)
		ClearStreamTable(receive)
	})
	gpc_r.Close()
	assert.True(t, gpc_r.IsClosed())
}

func createStreamDeserializer_r2() (sdHandler1_r, sdBatchHandler1_r) {
	_, err := gcConn_r.RunScript(
		`st2_gpc_r = streamTable(100:0, 'timestampv''sym''blob''price1',[TIMESTAMP,SYMBOL,BLOB,DOUBLE]);
		enableTableShareAndPersistence(table=st2_gpc_r, tableName='SDoutTables_gpc_r', asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0);
		go;
		setStreamTableFilterColumn(SDoutTables_gpc_r, 'sym')`)
	AssertNil(err)
	_, err = gcConn_r.RunScript(
		`n = 1000;
		t0 = table(100:0, "datetimev""timestampv""sym""price1""price2", [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);
		share t0 as table1_gpc_r;
		t = table(100:0, "datetimev""timestampv""sym""price1", [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);
		tableInsert(table1_gpc_r, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take("a1""b1""c1",n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));
		tableInsert(t, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take("a1""b1""c1",n), rand(100,n)+rand(1.0, n));
		dbpath="dfs://test_dfs";if(existsDatabase(dbpath)){dropDatabase(dbpath)};db=database(dbpath, VALUE, "a1""b1""c1");
		db.createPartitionedTable(t,"table2_gpc_r","sym").append!(t);
		t2 = select * from loadTable(dbpath,"table2_gpc_r");share t2 as table2_gpc_r;
		d = dict(['msg1','msg2'], [table1_gpc_r, table2_gpc_r]);
		replay(inputTables=d, outputTables="SDoutTables_gpc_r", dateColumn="timestampv", timeColumn="timestampv")`)
	AssertNil(err)
	sdMap := make(map[string][2]string)
	sdMap["msg1"] = [2]string{"", "table1_gpc_r"}
	sdMap["msg2"] = [2]string{"dfs://test_dfs", "table2_gpc_r"}
	opt := streaming.StreamDeserializerOption{
		TableNames: sdMap,
		Conn:       gcConn_r,
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
	sh := sdHandler1_r{*sd, 0, 0, args1, args2, ex_types1, ex_types2, plock1}
	sbh := sdBatchHandler1_r{*sd, 0, 0, args1, args2, ex_types1, ex_types2, plock2}
	fmt.Println("create handler successfully.")
	return sh, sbh
}

func TestNewGoroutinePooledClient_subscribe_with_StreamDeserializer_r(t *testing.T) {
	var gpc_r = streaming.NewGoroutinePooledClient(setup.IP, setup.Reverse_subPort)

	Convey("TestNewGoroutinePooledClient_subscribe_onehandler_with_StreamDeserializer", t, func() {
		_, err := gpcConn_r.RunScript(
			"try{ dropStreamTable(`SDoutTables_gpc_r);}catch(ex){};" +
				"try{ dropStreamTable(`st2_gpc_r);}catch(ex){};" +
				"try{ undef(`table1_gpc_r, SHARED);}catch(ex){};" +
				"try{ undef(`table2_gpc_r, SHARED);}catch(ex){};go")
		So(err, ShouldBeNil)
		sdhandler, _ := createStreamDeserializer_r2()
		req1 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "SDoutTables_gpc_r",
			ActionName: "testStreamDeserializer",
			Offset:     0,
			Handler:    &sdhandler,
			Reconnect:  true,
		}

		targetows := 2000
		err = gpc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		fmt.Println("started subscribe...")
		for {
			time.Sleep(1 * time.Second)
			if sdhandler.msg1_total+sdhandler.msg2_total == targetows {
				break
			} else {
				fmt.Println(sdhandler.msg1_total + sdhandler.msg2_total)

			}
		}
		err = gpc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)

		res_tab1 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1", "price2"}, sdhandler.res1_data)
		res_tab2 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1"}, sdhandler.res2_data)

		gpcConn_r.Upload(map[string]model.DataForm{"res1": res_tab1, "res2": res_tab2})
		ans1, err := gpcConn_r.RunScript("res = select * from res1 order by datetimev,timestampv;ex= select * from table1_gpc_r order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans1.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}

		ans2, err := gpcConn_r.RunScript("res = select * from res2 order by datetimev,timestampv;ex= select * from table2_gpc_r order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans2.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}
		_, err = gpcConn_r.RunScript(
			"try{ dropStreamTable(`SDoutTables_gpc_r);}catch(ex){};" +
				"try{ dropStreamTable(`st2_gpc_r);}catch(ex){};" +
				"try{ undef(`table1_gpc_r, SHARED);}catch(ex){};" +
				"try{ undef(`table2_gpc_r, SHARED);}catch(ex){};go")
		So(err, ShouldBeNil)

	})
	Convey("TestNewGoroutinePooledClient_subscribe_batchHandler_with_StreamDeserializer", t, func() {
		_, err := gpcConn_r.RunScript(
			"try{ dropStreamTable(`SDoutTables_gpc_r);}catch(ex){};" +
				"try{ dropStreamTable(`st2_gpc_r);}catch(ex){};" +
				"try{ undef(`table1_gpc_r, SHARED);}catch(ex){};" +
				"try{ undef(`table2_gpc_r, SHARED);}catch(ex){};go")
		So(err, ShouldBeNil)
		_, sdBatchHandler1 := createStreamDeserializer_r2()
		req1 := &streaming.SubscribeRequest{
			Address:      setup.Address,
			TableName:    "SDoutTables_gpc_r",
			ActionName:   "testStreamDeserializer",
			Offset:       0,
			BatchHandler: &sdBatchHandler1,
			Reconnect:    true,
		}
		req1.SetBatchSize(500)
		targetows := 2000
		err = gpc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		fmt.Println("started subscribe...")
		for {
			time.Sleep(1 * time.Second)
			if sdBatchHandler1.msg1_total+sdBatchHandler1.msg2_total == targetows {
				break
			} else {
				fmt.Println(sdBatchHandler1.msg1_total + sdBatchHandler1.msg2_total)
			}
		}
		err = gpc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)

		res_tab1 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1", "price2"}, sdBatchHandler1.res1_data)
		res_tab2 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1"}, sdBatchHandler1.res2_data)

		gpcConn_r.Upload(map[string]model.DataForm{"res1": res_tab1, "res2": res_tab2})
		ans1, err := gpcConn_r.RunScript("res = select * from res1 order by datetimev,timestampv;ex= select * from table1_gpc_r order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans1.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}

		ans2, err := gpcConn_r.RunScript("res = select * from res2 order by datetimev,timestampv;ex= select * from table2_gpc_r order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans2.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}
		_, err = gpcConn_r.RunScript(
			"try{ dropStreamTable(`SDoutTables_gpc_r);}catch(ex){};" +
				"try{ dropStreamTable(`st2_gpc_r);}catch(ex){};" +
				"try{ undef(`table1_gpc_r, SHARED);}catch(ex){};" +
				"try{ undef(`table2_gpc_r, SHARED);}catch(ex){};go")
		So(err, ShouldBeNil)

	})
	gpc_r.Close()
	assert.True(t, gpc_r.IsClosed())
}
