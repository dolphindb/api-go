package test

import (
	"context"
	"fmt"
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

var host3 = getRandomClusterAddress()
var gpcConn, _ = api.NewSimpleDolphinDBClient(context.TODO(), host3, setup.UserName, setup.Password)

func TestNewGoroutinePooledClient_subscribe_ex_ubsubscribe(t *testing.T) {
	var gpc = streaming.NewGoroutinePooledClient(setup.IP, setup.SubPort)
	Convey("TestNewGoroutinePooledClient_subscribe_ex_ubsubscribe", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gpcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gpcConn,
		}

		req := &streaming.SubscribeRequest{
			Address:      host3,
			TableName:    st,
			ActionName:   "subTradesTable",
			BatchHandler: &handler,
		}
		req.SetBatchSize(2).SetThrottle(1)
		err := gpc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(gpcConn, receive, 1000)
		tmp1, err := gpcConn.RunScript("select * from " + receive + " order by tag")
		So(err, ShouldBeNil)
		tmp2, err := gpcConn.RunScript("select * from " + st + " order by tag")
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("dropStreamTable('" + st + "');dropStreamTable('" + receive + "')")
		So(err, ShouldNotBeNil)
		re := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		CheckmodelTableEqual(re, ex, 0)
		err = gpc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host3, st)
		ClearStreamTable(host3, receive)
	})
	gpc.Close()
	assert.True(t, gpc.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_ex_ActionName(t *testing.T) {
	var gpc = streaming.NewGoroutinePooledClient(setup.IP, setup.SubPort)
	Convey("TestNewGoroutinePooledClient_subscribe_ex_ActionName", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gpcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gpcConn,
		}
		req := &streaming.SubscribeRequest{
			Address:      host3,
			TableName:    st,
			Offset:       0,
			Reconnect:    true,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err := gpc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(gpcConn, receive, 1000)
		err = gpc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host3, st)
		ClearStreamTable(host3, receive)
	})
	gpc.Close()
	assert.True(t, gpc.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_exTableName(t *testing.T) {
	var gpc = streaming.NewGoroutinePooledClient(setup.IP, setup.SubPort)
	Convey("TestNewGoroutinePooledClient_subscribe_exTableName", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gpcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gpcConn,
		}
		req := &streaming.SubscribeRequest{
			Address:      host3,
			Offset:       0,
			Reconnect:    true,
			BatchHandler: &handler,
		}
		err := gpc.Subscribe(req)
		So(err, ShouldNotBeNil)
		ClearStreamTable(host3, st)
		ClearStreamTable(host3, receive)
	})
	gpc.Close()
	assert.True(t, gpc.IsClosed())
}
func TestNewGoroutinePooledClient_subscribe_ex_offset(t *testing.T) {
	var gpc = streaming.NewGoroutinePooledClient(setup.IP, setup.SubPort)
	Convey("TestNewGoroutinePooledClient_subscribe_ex_offset", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gpcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gpcConn,
		}
		req := &streaming.SubscribeRequest{
			Address:      host3,
			TableName:    st,
			ActionName:   "subTradesTable",
			Offset:       -2,
			Reconnect:    true,
			BatchHandler: &handler,
		}
		err := gpc.Subscribe(req)
		So(err, ShouldNotBeNil)
		err = gpc.UnSubscribe(req)
		AssertNil(err)
		ClearStreamTable(host3, st)
		ClearStreamTable(host3, receive)
	})
	gpc.Close()
	assert.True(t, gpc.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_offset_0(t *testing.T) {
	var gpc = streaming.NewGoroutinePooledClient(setup.IP, setup.SubPort)
	Convey("TestNewGoroutinePooledClient_subscribe_offset_0", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gpcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gpcConn,
		}
		req := &streaming.SubscribeRequest{
			Address:      host3,
			TableName:    st,
			ActionName:   "subTradesTable",
			Offset:       0,
			Reconnect:    true,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err := gpc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(gpcConn, receive, 1000)

		res, _ := gpcConn.RunScript("res = select * from " + receive + " order by tag;ex = select * from " + st + " order by tag;each(eqObj, ex.values(), res.values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}
		err = gpc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host3, st)
		ClearStreamTable(host3, receive)
	})
	gpc.Close()
	assert.True(t, gpc.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_offset_negative(t *testing.T) {
	var gpc = streaming.NewGoroutinePooledClient(setup.IP, setup.SubPort)
	Convey("TestNewGoroutinePooledClient_subscribe_offset_negative", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gpcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gpcConn,
		}
		req := &streaming.SubscribeRequest{
			Address:      host3,
			TableName:    st,
			ActionName:   "subTradesTable",
			Offset:       -1,
			Reconnect:    true,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		_, err := gpcConn.RunScript("n=1000;t1=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t1)")
		So(err, ShouldBeNil)

		err = gpc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=2000;t2=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t2)")
		So(err, ShouldBeNil)
		waitData(gpcConn, receive, 2000)
		res, err := gpcConn.RunScript("res = select * from " + receive + " order by tag;ex = select * from t2 order by tag;each(eqObj, ex.values(), res.values())")
		AssertNil(err)
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}
		err = gpc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host3, st)
		ClearStreamTable(host3, receive)
	})
	gpc.Close()
	assert.True(t, gpc.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_offset_10(t *testing.T) {
	var gpc = streaming.NewGoroutinePooledClient(setup.IP, setup.SubPort)
	Convey("TestNewGoroutinePooledClient_subscribe_offset_10", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gpcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gpcConn,
		}
		_, err := gpcConn.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host3,
			TableName:    st,
			ActionName:   "subTradesTable",
			Offset:       10,
			Reconnect:    true,
			BatchHandler: &handler,
		}
		req.SetBatchSize(1000)
		err = gpc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(gpcConn, receive, 1090)
		tmp1, err := gpcConn.RunScript("select * from " + receive + " order by tag")
		So(err, ShouldBeNil)
		tmp2, err := gpcConn.RunScript("select * from " + st + " where rowNo(tag)>=10 order by tag")
		So(err, ShouldBeNil)
		re := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		So(re.Rows(), ShouldEqual, 1090)
		So(ex.Rows(), ShouldEqual, 1090)
		err = gpc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host3, st)
		ClearStreamTable(host3, receive)
	})
	gpc.Close()
	assert.True(t, gpc.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_offset_morethanowCount(t *testing.T) {
	var gpc = streaming.NewGoroutinePooledClient(setup.IP, setup.SubPort)
	Convey("TestNewGoroutinePooledClient_subscribe_offset_morethanowCount", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gpcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gpcConn,
		}
		_, err := gpcConn.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:      host3,
			TableName:    st,
			ActionName:   "subTradesTable",
			Offset:       1000,
			Reconnect:    true,
			BatchHandler: &handler,
		}
		err = gpc.Subscribe(req)
		So(err, ShouldNotBeNil)
		err = gpc.UnSubscribe(req)
		AssertNil(err)
		ClearStreamTable(host3, st)
		ClearStreamTable(host3, receive)
	})
	gpc.Close()
	assert.True(t, gpc.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_filter(t *testing.T) {
	var gpc = streaming.NewGoroutinePooledClient(setup.IP, setup.SubPort)
	Convey("TestNewGoroutinePooledClient_subscribe_filter", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gpcConn)
		handler := MessageHandler{
			receive: receive,
			conn:    gpcConn,
		}
		script2 := "try{dropStreamTable('st3')}catch(ex){};try{dropStreamTable('" + receive + "')}catch(ex){};go;tmp3 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]);" +
			"enableTableShareAndPersistence(table=tmp3, tableName=`" + receive + ", asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n"
		_, err := gpcConn.RunScript(script2)
		So(err, ShouldBeNil)
		filter1, err := gpcConn.RunScript("1..1000")
		So(err, ShouldBeNil)
		filter2, err := gpcConn.RunScript("2001..3000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:    host3,
			TableName:  st,
			ActionName: "subTradesTable1",
			Offset:     0,
			Reconnect:  true,
			Filter:     filter1.(*model.Vector),
			Handler:    &handler,
		}
		req2 := &streaming.SubscribeRequest{
			Address:    host3,
			TableName:  st,
			ActionName: "subTradesTable2",
			Offset:     0,
			Reconnect:  true,
			Filter:     filter2.(*model.Vector),
			Handler:    &handler,
		}
		err = gpc.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=4000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		tmp1, err := gpcConn.RunScript("select * from " + receive + " order by tag, ts, data")
		So(err, ShouldBeNil)
		waitData(gpcConn, receive, 1000)
		tmp3, err := gpcConn.RunScript("select * from " + receive + " order by tag, ts, data")
		So(err, ShouldBeNil)
		So(tmp3.Rows(), ShouldEqual, 1000)
		CheckmodelTableEqual(tmp1.(*model.Table), tmp3.(*model.Table), 1000)

		err = gpc.Subscribe(req2)
		So(err, ShouldBeNil)
		waitData(gpcConn, receive, 1000)
		tmp3, err = gpcConn.RunScript("select * from " + receive + " order by tag, ts, data")
		So(err, ShouldBeNil)
		So(tmp3.Rows(), ShouldEqual, 1000)
		CheckmodelTableEqual(tmp1.(*model.Table), tmp3.(*model.Table), 2000)
		err = gpc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		err = gpc.UnSubscribe(req2)
		So(err, ShouldBeNil)
		ClearStreamTable(host3, st)
		ClearStreamTable(host3, receive)
	})
	gpc.Close()
	assert.True(t, gpc.IsClosed())
}

func TestNewGoroutinePooledClient_batchSize_throttle(t *testing.T) {
	var gpc = streaming.NewGoroutinePooledClient(setup.IP, setup.SubPort)
	Convey("TestNewGoroutinePooledClient_batchSize_throttle", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gpcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gpcConn,
		}
		filter1, err := gpcConn.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:      host3,
			TableName:    st,
			ActionName:   "subTradesTable1",
			Offset:       -1,
			Filter:       filter1.(*model.Vector),
			BatchHandler: &handler,
			Reconnect:    true,
		}
		req1.SetBatchSize(1000).SetThrottle(5)
		err = gpc.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(gpcConn, receive, 1000)
		res, err := gpcConn.RunScript("res = select * from " + receive + " order by tag;ex = select * from " + st + " where tag between 1:1000 order by tag;each(eqObj, ex.values(), res.values())")
		AssertNil(err)
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}
		err = gpc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		ClearStreamTable(host3, st)
		ClearStreamTable(host3, receive)

	})
	gpc.Close()
	assert.True(t, gpc.IsClosed())
}

func TestNewGoroutinePooledClient_batchSize_throttle2(t *testing.T) {
	var gpc = streaming.NewGoroutinePooledClient(setup.IP, setup.SubPort)
	Convey("TestNewGoroutinePooledClient_batchSize_throttle2", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gpcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gpcConn,
		}
		filter1, err := gpcConn.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:      host3,
			TableName:    st,
			ActionName:   "subTradesTable1",
			Offset:       -1,
			Filter:       filter1.(*model.Vector),
			BatchHandler: &handler,
			Reconnect:    true,
		}
		req1.SetBatchSize(10000).SetThrottle(5)
		err = gpc.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		waitData(gpcConn, receive, 200)
		tmp1, err := gpcConn.RunScript("select * from " + receive + " order by tag,ts,data")
		So(err, ShouldBeNil)
		tmp2, err := gpcConn.RunScript("select * from " + st + " order by tag,ts,data")
		So(err, ShouldBeNil)
		err = gpc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		re1 := tmp1.(*model.Table)
		ex := tmp2.(*model.Table)
		So(re1.Rows(), ShouldEqual, 200)
		CheckmodelTableEqual(re1, ex, 0)
		ClearStreamTable(host3, st)
		ClearStreamTable(host3, receive)
	})
	gpc.Close()
	assert.True(t, gpc.IsClosed())
}

func TestNewGoroutinePooledClient_tableName_handler_offseteconnect_success(t *testing.T) {
	var gpc = streaming.NewGoroutinePooledClient(setup.IP, setup.Reverse_subPort)
	Convey("TestNewGoroutinePooledClient_tableName_handler_offseteconnect_success", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gpcConn)
		handler := MessageHandler{
			receive: receive,
			conn:    gpcConn,
		}
		_, err := gpcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:   host3,
			TableName: st,
			Offset:    0,
			Reconnect: true,
			Handler:   &handler,
		}
		err = gpc.Subscribe(req)
		So(err, ShouldBeNil)

		_, err = gpcConn.RunScript("n=500;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		_, err = gpcConn.RunScript("stopPublishTable('" + setup.IP + "'," + strings.Split(host3, ":")[1] + ",'" + st + "')")
		So(err, ShouldBeNil)

		_, err = gpcConn.RunScript("n=500;t=table(1..n+500 as tag,now()+1..n+500 as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		_, err = gpcConn.RunScript("stopPublishTable('" + setup.IP + "'," + strings.Split(host3, ":")[1] + ",'" + st + "')")
		So(err, ShouldBeNil)

		time.Sleep(10 * time.Second)
		res, _ := gpcConn.RunScript("res = select * from " + receive + " order by tag;ex = select * from " + st + " order by tag;each(eqObj, ex.values(), res.values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}
		waitData(gpcConn, receive, 2000)
		err = gpc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host3, st)
		ClearStreamTable(host3, receive)
	})
	gpc.Close()
	assert.True(t, gpc.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_unsubscribeesubscribe(t *testing.T) {
	var gpc = streaming.NewGoroutinePooledClient(setup.IP, setup.SubPort)
	Convey("TestNewGoroutinePooledClient_subscribe_unsubscribeesubscribe", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gpcConn)
		handler := MessageBatchHandler{
			receive: receive,
			conn:    gpcConn,
		}
		filter1, err := gpcConn.RunScript("1..1000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:      host3,
			TableName:    st,
			ActionName:   "subTradesTable1",
			Offset:       -1,
			Filter:       filter1.(*model.Vector),
			BatchHandler: &handler,
			Reconnect:    true,
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
		ClearStreamTable(host3, st)
		ClearStreamTable(host3, receive)
	})
	gpc.Close()
	assert.True(t, gpc.IsClosed())
}

func TestNewGoroutinePooledClient_msgAsTable(t *testing.T) {
	var gpc = streaming.NewGoroutinePooledClient(setup.IP, setup.SubPort)
	Convey("TestNewGoroutinePooledClient_msgAsTable", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gpcConn)
		handler := MessageHandler_table{
			receive: receive,
			conn:    gpcConn,
		}
		req1 := &streaming.SubscribeRequest{
			Address:    host3,
			TableName:  st,
			ActionName: "subTrades1",
			Offset:     0,
			Handler:    &handler,
			Reconnect:  true,
			MsgAsTable: true,
		}
		req1.SetBatchSize(1000)
		err := gpc.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = gpcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		for {
			res, err := gpcConn.RunScript("exec * from " + receive)
			So(err, ShouldBeNil)
			if res.Rows() == 1000 {
				break
			}
		}
		err = gpc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		ClearStreamTable(host3, st)
		ClearStreamTable(host3, receive)
	})
	gpc.Close()
	assert.True(t, gpc.IsClosed())
}

func TestNewGoroutinePooledClient_subscribe_with_StreamDeserializer(t *testing.T) {
	var gpc = streaming.NewGoroutinePooledClient(setup.IP, setup.SubPort)

	Convey("TestNewGoroutinePooledClient_subscribe_onehandler_with_StreamDeserializer", t, func() {
		sdhandler, _ := createStreamDeserializer(gpcConn, "SDoutTables_gpc")
		req1 := &streaming.SubscribeRequest{
			Address:    host3,
			TableName:  "SDoutTables_gpc",
			ActionName: "testStreamDeserializer",
			Offset:     0,
			Handler:    &sdhandler,
			Reconnect:  true,
		}

		targetows := 2000
		err := gpc.Subscribe(req1)
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
		err = gpc.UnSubscribe(req1)
		So(err, ShouldBeNil)

		res_tab1 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1", "price2"}, sdhandler.res1_data)
		res_tab2 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1"}, sdhandler.res2_data)

		gpcConn.Upload(map[string]model.DataForm{"res1": res_tab1, "res2": res_tab2})
		ans1, err := gpcConn.RunScript("res = select * from res1 order by datetimev,timestampv;ex= select * from table1 order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans1.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}

		ans2, err := gpcConn.RunScript("res = select * from res2 order by datetimev,timestampv;ex= select * from table2 order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans2.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}
		_, err = gpcConn.RunScript(
			"try{ dropStreamTable(`SDoutTables_gpc);}catch(ex){};" +
				"try{ dropStreamTable(`st2);}catch(ex){};" +
				"try{ undef(`table1, SHARED);}catch(ex){};" +
				"try{ undef(`table2, SHARED);}catch(ex){};go")
		So(err, ShouldBeNil)

	})
	Convey("TestNewGoroutinePooledClient_subscribe_batchHandler_with_StreamDeserializer", t, func() {
		_, sdBatchHandler := createStreamDeserializer(gpcConn, "SDoutTables_gpc")
		req1 := &streaming.SubscribeRequest{
			Address:      host3,
			TableName:    "SDoutTables_gpc",
			ActionName:   "testStreamDeserializer",
			Offset:       0,
			BatchHandler: &sdBatchHandler,
			Reconnect:    true,
		}
		req1.SetBatchSize(500)
		targetows := 2000
		err := gpc.Subscribe(req1)
		So(err, ShouldBeNil)
		fmt.Println("started subscribe...")
		for {
			time.Sleep(1 * time.Second)
			if sdBatchHandler.msg1_total+sdBatchHandler.msg2_total == targetows {
				break
			} else {
				fmt.Println(sdBatchHandler.msg1_total + sdBatchHandler.msg2_total)
			}
		}
		err = gpc.UnSubscribe(req1)
		So(err, ShouldBeNil)

		res_tab1 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1", "price2"}, sdBatchHandler.res1_data)
		res_tab2 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1"}, sdBatchHandler.res2_data)

		gpcConn.Upload(map[string]model.DataForm{"res1": res_tab1, "res2": res_tab2})
		ans1, err := gpcConn.RunScript("res = select * from res1 order by datetimev,timestampv;ex= select * from table1 order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans1.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}

		ans2, err := gpcConn.RunScript("res = select * from res2 order by datetimev,timestampv;ex= select * from table2 order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans2.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}
		_, err = gpcConn.RunScript(
			"try{ dropStreamTable(`SDoutTables_gpc);}catch(ex){};" +
				"try{ dropStreamTable(`st2);}catch(ex){};" +
				"try{ undef(`table1, SHARED);}catch(ex){};" +
				"try{ undef(`table2, SHARED);}catch(ex){};go")
		So(err, ShouldBeNil)

	})
	gpc.Close()
	assert.True(t, gpc.IsClosed())
}

func TestNewGoroutinePooledClient_unsubscribe_in_doEvent(t *testing.T) {
	var gpc = streaming.NewGoroutinePooledClient(setup.IP, setup.SubPort)
	Convey("TestNewGoroutinePooledClient_unsubscribe_in_doEvent", t, func() {
		st, receive := CreateStreamingTableWithRandomName(gpcConn)
		handler := MessageHandler_unsubscribeInDoEvent{
			subType:   "gpc",
			subClient: gpc,
			subReq: &streaming.SubscribeRequest{
				Address:    host3,
				TableName:  st,
				ActionName: "subTrades1",
				Offset:     0},
			successCount: 0,
		}
		_, err := gpcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    host3,
			TableName:  st,
			ActionName: "subTrades1",
			Offset:     0,
			Reconnect:  true,
			Handler:    &handler,
		}
		err = gpc.Subscribe(req)
		So(err, ShouldBeNil)

		res_inSub, _ := gpcConn.RunScript("getStreamingStat().pubConns")
		// fmt.Println(res_inSub)
		time.Sleep(8 * time.Second)
		res_afterSub, _ := gpcConn.RunScript("getStreamingStat().pubConns")
		// fmt.Println(res_afterSub)
		So(res_inSub.(*model.Table).GetColumnByName("tables").String(), ShouldContainSubstring, st)
		So(res_afterSub.(*model.Table).GetColumnByName("tables").String(), ShouldNotContainSubstring, st)
		So(handler.successCount, ShouldBeGreaterThan, 1)

		ClearStreamTable(host3, st)
		ClearStreamTable(host3, receive)
	})
	gpc.Close()
	assert.True(t, gpc.IsClosed())
}
