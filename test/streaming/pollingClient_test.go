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

var host5 = getRandomClusterAddress()
var pcConn, _ = api.NewSimpleDolphinDBClient(context.TODO(), host5, setup.UserName, setup.Password)

func TestSubscribe_exception(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)
	Convey("Test_subscribe_exception", t, func() {
		Convey("Test_AbstractClient_shared_table_polling_doesnot_exist_exception", func() {
			req := &streaming.SubscribeRequest{
				Address:    host5,
				TableName:  "errtab",
				ActionName: "action1",
				Offset:     0,
				Reconnect:  true,
			}
			_, err := pc.Subscribe(req)
			So(err.Error(), ShouldContainSubstring, "shared table errtab doesn't exist")
		})
		st, receive := CreateStreamingTableWithRandomName(pcConn)
		Convey("Test_subscribe_err_host", func() {
			req := &streaming.SubscribeRequest{
				Address:    "999.0.0.1:8876",
				TableName:  st,
				ActionName: "action1",
				Offset:     0,
				Reconnect:  true,
			}
			_, err := pc.Subscribe(req)
			So(err.Error(), ShouldContainSubstring, "failed to connect to")
		})
		Convey("Test_subscribe_err_port", func() {
			req := &streaming.SubscribeRequest{
				Address:    setup.IP + ":0001",
				TableName:  st,
				ActionName: "action1",
				Offset:     0,
				Reconnect:  true,
			}
			_, err := pc.Subscribe(req)
			So(err.Error(), ShouldContainSubstring, "failed to connect to")
		})
		Convey("Test_subscribe_err_TableName", func() {
			req := &streaming.SubscribeRequest{
				Address:    host5,
				TableName:  "",
				ActionName: "action1",
				Offset:     0,
				Reconnect:  true,
			}
			_, err := pc.Subscribe(req)
			So(err.Error(), ShouldContainSubstring, "Illegal table name")
		})
		Convey("Test_subscribe_ActionName_null", func() {
			req := &streaming.SubscribeRequest{
				Address:    host5,
				ActionName: "",
				TableName:  st,
				Offset:     0,
				Reconnect:  true,
			}
			_, err := pc.Subscribe(req)
			So(err, ShouldBeNil)
			pc.UnSubscribe(req)
		})
		Convey("Test_subscribe_AllowExists", func() {
			req := &streaming.SubscribeRequest{
				Address:     host5,
				ActionName:  "AllowExists_test",
				TableName:   st,
				Offset:      0,
				Reconnect:   true,
				AllowExists: true,
			}
			_, err := pc.Subscribe(req)
			So(err, ShouldBeNil)
			pc.UnSubscribe(req)
			_, err2 := pc.Subscribe(req)
			So(err2, ShouldBeNil)
			pc.UnSubscribe(req)

			req2 := &streaming.SubscribeRequest{
				Address:     host5,
				ActionName:  "AllowExists_test2_pc",
				TableName:   st,
				Offset:      0,
				Reconnect:   true,
				AllowExists: false,
			}
			_, err3 := pc.Subscribe(req2)
			So(err3, ShouldBeNil)
			pc.UnSubscribe(req)
			_, err4 := pc.Subscribe(req2)
			So(err4.Error(), ShouldContainSubstring, "already be subscribed")
			pc.UnSubscribe(req2)

			ClearStreamTable(host5, st)
			ClearStreamTable(host5, receive)
		})
	})
	pc.Close()
	assert.True(t, pc.IsClosed())
}

func TestPollingClient(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)
	Convey("Test_PollingClient_test_size", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host5, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		st, receive := CreateStreamingTableWithRandomName(pcConn)
		req := &streaming.SubscribeRequest{
			Address:    host5,
			TableName:  st,
			ActionName: "action1",
			Offset:     0,
			Reconnect:  true,
		}
		poller, err := pc.Subscribe(req)
		So(err, ShouldBeNil)
		Convey("Test_GetTopicPoller_exitsing_data", func() {
			msg := poller.Poll(1000, 10)
			So(len(msg), ShouldEqual, 0)
		})
		Convey("poll size>data", func() {
			for i := 0; i < 10; i++ { //data<size
				_, err = ddb.RunScript("insert into " + st + " values(rand(100, 50), take(now(), 50), rand(1000,50)/10.0);")
				AssertNil(err)
				msgs := poller.Poll(100, 1000)
				count := 0
				for _, msg := range msgs {
					if msg == nil {
						continue
					} else {
						count += 1
					}
				}
				So(count, ShouldBeIn, []int{0, 50, 100})

			}
		})
		Convey("poll size<data", func() {
			for i := 0; i < 10; i++ { //data>size
				_, err = ddb.RunScript("dataNum=5000;insert into " + st + " values(rand(100, dataNum), take(now(), dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				msgs := poller.Poll(100000, 1000)
				count := 0
				for _, msg := range msgs {
					if msg == nil {
						continue
					} else {
						count += 1
					}
				}
				So(count, ShouldEqual, 1000)
			}
		})
		Convey("poll size=data", func() {
			for i := 0; i < 10; i++ { //data=size
				_, err = ddb.RunScript("dataNum=5000;insert into " + st + " values(rand(100, dataNum), take(now(), dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				msgs := poller.Poll(100000, 5000)
				count := 0
				for _, msg := range msgs {
					if msg == nil {
						continue
					} else {
						count += 1
					}
				}
				So(count, ShouldEqual, 5000)
			}
		})
		Convey("poll bigsize", func() {
			for i := 0; i < 10; i++ { //bigsize
				_, err = ddb.RunScript("dataNum=5000;insert into " + st + " values(rand(100, dataNum), take(now(), dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				msgs := poller.Poll(1000, 1000000000)
				count := 0
				for _, msg := range msgs {
					if msg == nil {
						continue
					} else {
						count += 1
					}
				}
				So(count, ShouldEqual, 5000)
			}
		})
		Convey("poll bigdata", func() {
			for i := 0; i < 10; i++ { //bigData
				_, err = ddb.RunScript("dataNum=10000000;insert into " + st + " values(rand(100, dataNum), take(now(), dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				msgs := poller.Poll(1000, 10000)
				count := 0
				for _, msg := range msgs {
					if msg == nil {
						continue
					} else {
						count += 1
					}
				}
				So(count, ShouldEqual, 10000)
			}
		})
		Convey("poll 1row", func() {
			for i := 0; i < 10; i++ { //smallData
				_, err = ddb.RunScript("dataNum=1;insert into " + st + " values(rand(100, dataNum), take(now(), dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				msgs := poller.Poll(1000, 10000)
				count := 0
				for _, msg := range msgs {
					if msg == nil {
						continue
					} else {
						count += 1
					}
				}
				So(count, ShouldEqual, 1)
			}
		})
		Convey("poll when inserting mangtimes", func() {
			for i := 0; i < 10; i++ { //append Many times
				_, err = ddb.RunScript("dataNum=10;insert into " + st + " values(rand(100, dataNum), take(now(), dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				_, err = ddb.RunScript("dataNum=20;insert into " + st + " values(rand(100, dataNum), take(now(), dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				_, err = ddb.RunScript("dataNum=30;insert into " + st + " values(rand(100, dataNum), take(now(), dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				_, err = ddb.RunScript("dataNum=40;insert into " + st + " values(rand(100, dataNum), take(now(), dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				_, err = ddb.RunScript("dataNum=50;insert into " + st + " values(rand(100, dataNum), take(now(), dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				msgs := poller.Poll(1000, 10000)
				count := 0
				for _, msg := range msgs {
					if msg == nil {
						continue
					} else {
						count += 1
					}
				}
				So(count, ShouldEqual, 150)
			}
		})
		Convey("poll size=0", func() {
			for i := 0; i < 10; i++ { //size=0
				_, err = ddb.RunScript("dataNum=100;insert into " + st + " values(rand(100, dataNum), take(now(), dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				msgs := poller.Poll(1000, 0)
				count := 0
				for _, msg := range msgs {
					if msg == nil {
						continue
					} else {
						count += 1
					}
				}
				So(count, ShouldEqual, 0)
			}
		})
		Convey("poll size<0", func() {
			for i := 0; i < 10; i++ { //size<0
				_, err = ddb.RunScript("dataNum=100;insert into " + st + " values(rand(100, dataNum), take(now(), dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				msgs := poller.Poll(1000, -10)
				count := 0
				for _, msg := range msgs {
					if msg == nil {
						continue
					} else {
						count += 1
					}
				}
				So(count, ShouldEqual, 0)
			}
		})
		err = pc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host5, st)
		ClearStreamTable(host5, receive)
	})
	pc.Close()
	assert.True(t, pc.IsClosed())
}

func TestSubsribe_size(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)
	Convey("TestSubsribe_size", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host5, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		st, receive := CreateStreamingTableWithRandomName(pcConn)
		req1 := &streaming.SubscribeRequest{
			Address:    host5,
			TableName:  st,
			ActionName: "subtrades1",
			Offset:     0,
			Reconnect:  true,
		}
		poller1, err := pc.Subscribe(req1)
		So(err, ShouldBeNil)
		So(err, ShouldBeNil)
		for i := 0; i < 10; i++ {
			_, err = ddb.RunScript("dataNum=1000;insert into " + st + " values(rand(100, dataNum), take(now(), dataNum), rand(1000,dataNum)/10.0);")
			So(err, ShouldBeNil)
			rows, _ := ddb.RunScript("exec count(*) from " + st + "")
			fmt.Print(rows)
			msg1 := poller1.Poll(1000, 1000)
			if msg1 == nil {
				continue
			} else if len(msg1) > 0 {
				So(len(msg1), ShouldBeGreaterThanOrEqualTo, 1000)
			}

		}
		err = pc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		So(ddb.Close(), ShouldBeNil)
		ClearStreamTable(host5, st)
		ClearStreamTable(host5, receive)
	})
	pc.Close()
	assert.True(t, pc.IsClosed())
}

func TestSubsribe_take(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)
	Convey("TestSubsribe_take", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host5, setup.UserName, setup.Password)
		st, receive := CreateStreamingTableWithRandomName(pcConn)
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    host5,
			TableName:  st,
			ActionName: "subtrades1",
			Offset:     0,
			Reconnect:  false,
		}
		poller3, err := pc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dataNum=1; insert into " + st + " values(rand(100, dataNum), take(now(), dataNum), rand(1000,dataNum)/10.0);")
		So(err, ShouldBeNil)
		IMessage := poller3.Take()
		Topicmsg := IMessage.GetTopic()
		fmt.Println(Topicmsg)
		SubscriptionTopic, err := ddb.RunScript("getSubscriptionTopic(tableName=\"" + st + "\", actionName=\"subtrades1\")")
		exTopic := SubscriptionTopic.(*model.Vector).Get(0).String()
		So(err, ShouldBeNil)
		So(exTopic, ShouldEqual, "string("+Topicmsg+")")
		Offset := IMessage.GetOffset()
		So(Offset, ShouldEqual, 0)
		tmp, err := ddb.RunScript("select * from " + st)
		exTable := tmp.(*model.Table)
		So(err, ShouldBeNil)
		retime := IMessage.GetValue(0).(*model.Scalar).String()
		resymbol := IMessage.GetValue(1).(*model.Scalar).String()
		reprice := IMessage.GetValue(2).(*model.Scalar).String()
		extimev := exTable.GetColumnByIndex(0).Get(0).String()
		exsymbol := exTable.GetColumnByIndex(1).Get(0).String()
		expricev := exTable.GetColumnByIndex(2).Get(0).String()
		retime1 := IMessage.GetValueByName("tag").String()
		resymbol1 := IMessage.GetValueByName("ts").String()
		reprice1 := IMessage.GetValueByName("data").String()
		So(retime, ShouldEqual, "int("+extimev+")")
		So(resymbol, ShouldEqual, "timestamp("+exsymbol+")")
		So(reprice, ShouldEqual, "double("+expricev+")")
		So(retime1, ShouldEqual, "int("+extimev+")")
		So(resymbol1, ShouldEqual, "timestamp("+exsymbol+")")
		So(reprice1, ShouldEqual, "double("+expricev+")")
		err = pc.UnSubscribe(req)
		So(err, ShouldBeNil)
		So(ddb.Close(), ShouldBeNil)
		ClearStreamTable(host5, st)
		ClearStreamTable(host5, receive)
	})
	pc.Close()
	assert.True(t, pc.IsClosed())
}

func TestPollingClient_bachSize_throttle(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)
	Convey("test_NewPollingClient_batchSize_lt0", t, func() {
		st, receive := CreateStreamingTableWithRandomName(pcConn)
		req := &streaming.SubscribeRequest{
			Address:    host5,
			TableName:  st,
			ActionName: "action1",
			Offset:     0,
			Reconnect:  true,
		}
		req.SetBatchSize(-1)
		q, err := pc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = pcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + st + ".append!(t)")
		So(err, ShouldBeNil)
		msgs := q.Poll(1000, 1000)
		for _, msg := range msgs {
			val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
			val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
			val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
			script := fmt.Sprintf("tableInsert(objByName(`"+receive+", true), %s,%s,%s)",
				val0, val1, val2)
			_, err := pcConn.RunScript(script)
			AssertNil(err)
		}
		res, _ := pcConn.RunScript("res = select * from " + receive + " order by tag;ex = select * from " + st + " order by tag;each(eqObj, ex.values(), res.values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}

		err = pc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host5, st)
		ClearStreamTable(host5, receive)
	})
	Convey("test_NewPollingClient_throttle_less_than_0", t, func() {
		st, receive := CreateStreamingTableWithRandomName(pcConn)

		req := &streaming.SubscribeRequest{
			Address:    host5,
			TableName:  st,
			ActionName: "action1",
			Offset:     0,
			Reconnect:  true,
		}
		req.SetBatchSize(500).SetThrottle(-10)
		q, err := pc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = pcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		msgs := q.Poll(1000, 1000)
		for _, msg := range msgs {
			val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
			val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
			val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
			script := fmt.Sprintf("tableInsert(objByName(`"+receive+", true), %s,%s,%s)",
				val0, val1, val2)
			_, err := pcConn.RunScript(script)
			AssertNil(err)
		}
		res, _ := pcConn.RunScript("res = select * from " + receive + " order by tag;ex = select * from " + st + " order by tag;each(eqObj, ex.values(), res.values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}

		err = pc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host5, st)
		ClearStreamTable(host5, receive)
	})
	pc.Close()
	assert.True(t, pc.IsClosed())
}

func TestPollingClient_tableName_offset(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)
	Convey("TestPollingClient_tableName_offset", t, func() {
		st, receive := CreateStreamingTableWithRandomName(pcConn)
		_, err := pcConn.RunScript("n=1000;t1=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t1)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    host5,
			TableName:  st,
			ActionName: "action1",
			Offset:     -1,
			Reconnect:  false,
		}
		req.SetBatchSize(1000)
		q, err := pc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = pcConn.RunScript("n=2000;t2=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t2)")
		So(err, ShouldBeNil)
		msgs := q.Poll(1000, 2000)
		for _, msg := range msgs {
			val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
			val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
			val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
			script := fmt.Sprintf("tableInsert(objByName(`"+receive+", true), %s,%s,%s)",
				val0, val1, val2)
			_, err := pcConn.RunScript(script)
			AssertNil(err)
		}
		res, _ := pcConn.RunScript("res = select * from " + receive + " order by tag;ex = select * from t2 order by tag;each(eqObj, ex.values(), res.values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}
		err = pc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host5, st)
		ClearStreamTable(host5, receive)
	})
	// TODO: offset = -2
}

func TestPollingClient_tableName_actionName(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)
	Convey("TestPollingClient_tableName_actionName", t, func() {
		st, receive := CreateStreamingTableWithRandomName(pcConn)
		_, err := pcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    host5,
			TableName:  st,
			ActionName: "test_actionName",
			Offset:     0,
			Reconnect:  false,
		}
		req.SetBatchSize(1000)
		_, err = pc.Subscribe(req)
		So(err, ShouldBeNil)
		res, err := pcConn.RunScript("getStreamingStat().pubTables")
		AssertNil(err)
		tableNames := res.(*model.Table).GetColumnByName("tableName").Data.Value()
		actions := res.(*model.Table).GetColumnByName("actions").Data.Value()
		So(tableNames, ShouldContain, st)
		So(actions, ShouldContain, "test_actionName")
		err = pc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host5, st)
		ClearStreamTable(host5, receive)
	})
	pc.Close()
	assert.True(t, pc.IsClosed())
}

func TestPollingClient_tableName_handler_offset_reconnect_success(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)
	Convey("TestPollingClient_tableName_handler_offseteconnect_success", t, func() {
		st, receive := CreateStreamingTableWithRandomName(pcConn)
		req := &streaming.SubscribeRequest{
			Address:   host5,
			TableName: st,
			Offset:    0,
			Reconnect: true,
		}
		q, err := pc.Subscribe(req)
		So(err, ShouldBeNil)

		_, err = pcConn.RunScript("n=500;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		_, err = pcConn.RunScript("stopPublishTable('" + setup.IP + "'," + strings.Split(host5, ":")[1] + ",'" + st + "')")
		So(err, ShouldBeNil)

		_, err = pcConn.RunScript("n=500;t=table(1..n+500 as tag,now()+1..n+500 as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		_, err = pcConn.RunScript("stopPublishTable('" + setup.IP + "'," + strings.Split(host5, ":")[1] + ",'" + st + "')")
		So(err, ShouldBeNil)
		time.Sleep(15 * time.Second)
		msgs := q.Poll(1000, 1000)
		for _, msg := range msgs {
			val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
			val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
			val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
			script := fmt.Sprintf("tableInsert(objByName(`"+receive+", true), %s,%s,%s)",
				val0, val1, val2)
			_, err := pcConn.RunScript(script)
			AssertNil(err)
		}
		res, _ := pcConn.RunScript("res = select * from " + receive + " order by tag;ex = select * from " + st + " order by tag;share res as res_t;share ex as ex_t;each(eqObj, ex.values(), res.values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}

		err = pc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host5, st)
		ClearStreamTable(host5, receive)
	})
	pc.Close()
	assert.True(t, pc.IsClosed())
}

func TestPollingClient_subscribe_offset_negative(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)
	Convey("TestPollingClient_subscribe_offset_negative", t, func() {
		st, receive := CreateStreamingTableWithRandomName(pcConn)
		_, err := pcConn.RunScript("n=1000;t1=table(1..n as tag,2020.01.04T12:23:45+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t1)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    host5,
			TableName:  st,
			ActionName: "sub" + st + "1",
			Offset:     -1,
		}
		req.SetBatchSize(1000)
		q, err := pc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = pcConn.RunScript("n=1000;t2=table(1..n+2000 as tag,2020.01.01T12:23:45+1..n+456 as ts,rand(100.0,n) as data);share t2 as s_t2;" + "" + st + ".append!(t2)")
		So(err, ShouldBeNil)
		msgs := q.Poll(1000, 1000)
		for _, msg := range msgs {
			val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
			val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
			val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
			script := fmt.Sprintf("tableInsert(objByName(`"+receive+", true), %s,%s,%s)",
				val0, val1, val2)
			_, err := pcConn.RunScript(script)
			AssertNil(err)
		}
		res, _ := pcConn.RunScript("res = select * from " + receive + " order by tag;ex = select * from " + st + " where tag >=2001 order by tag;each(eqObj, ex.values(), res.values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}

		err = pc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host5, st)
		ClearStreamTable(host5, receive)
	})
	pc.Close()
	assert.True(t, pc.IsClosed())
}

func TestPollingClient_subscribe_offset_10(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)
	Convey("TestPollingClient_subscribe_offset_10", t, func() {
		st, receive := CreateStreamingTableWithRandomName(pcConn)
		_, err := pcConn.RunScript("n=1000;t1=table(1..n as tag,2020.01.04T12:23:45+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t1)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    host5,
			TableName:  st,
			ActionName: "sub" + st + "1",
			Offset:     10,
		}
		req.SetBatchSize(1000)
		q, err := pc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = pcConn.RunScript("n=1000;t2=table(1..n+1000 as tag,2020.01.01T12:23:45+1..n+456 as ts,rand(100.0,n) as data);share t2 as s_t2;" + "" + st + ".append!(t2)")
		So(err, ShouldBeNil)
		msgs := q.Poll(1000, 2000)
		for _, msg := range msgs {
			val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
			val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
			val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
			script := fmt.Sprintf("tableInsert(objByName(`"+receive+", true), %s,%s,%s)",
				val0, val1, val2)
			_, err := pcConn.RunScript(script)
			AssertNil(err)
		}
		res, _ := pcConn.RunScript("res = select * from " + receive + " order by tag;ex = select * from " + st + " where tag > 10 order by tag;each(eqObj, ex.values(), res.values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}

		err = pc.UnSubscribe(req)
		So(err, ShouldBeNil)
		ClearStreamTable(host5, st)
		ClearStreamTable(host5, receive)
	})
	pc.Close()
	assert.True(t, pc.IsClosed())
}

func TestPollingClient_subscribe_offset_morethan_tableCount(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)
	Convey("TestPollingClient_subscribe_offset_morethan_tableCount", t, func() {
		st, receive := CreateStreamingTableWithRandomName(pcConn)
		var offset int64 = 1001
		_, err := pcConn.RunScript("n=1000;t=table(1..n as tag,2020.01.04T12:23:45+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    host5,
			TableName:  st,
			ActionName: "sub" + st + "1",
			Offset:     offset,
		}
		req.SetBatchSize(1000)
		_, err = pc.Subscribe(req)
		So(err.Error(), ShouldContainSubstring, "Failed to subscribe to table "+st+". Can't find the message with offset ["+strconv.Itoa(int(offset))+"].")
		err = pc.UnSubscribe(req)
		AssertNil(err)
		ClearStreamTable(host5, st)
		ClearStreamTable(host5, receive)
	})
	pc.Close()
	assert.True(t, pc.IsClosed())
}

func TestPollingClient_subscribe_filter(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)
	Convey("TestPollingClient_subscribe_filter", t, func() {
		st, receive := CreateStreamingTableWithRandomName(pcConn)
		filter1, err := pcConn.RunScript("1..1000")
		So(err, ShouldBeNil)
		filter2, err := pcConn.RunScript("2001..3000")
		So(err, ShouldBeNil)
		req1 := &streaming.SubscribeRequest{
			Address:    host5,
			TableName:  st,
			ActionName: "sub" + st + "1",
			Offset:     0,
			Filter:     filter1.(*model.Vector),
		}
		req2 := &streaming.SubscribeRequest{
			Address:    host5,
			TableName:  st,
			ActionName: "sub" + st + "2",
			Offset:     0,
			Filter:     filter2.(*model.Vector),
		}
		q, err := pc.Subscribe(req1)
		So(err, ShouldBeNil)
		q2, err := pc.Subscribe(req2)
		So(err, ShouldBeNil)
		_, err = pcConn.RunScript("n=4000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		time.Sleep(30 * time.Second)
		msgs := q.Poll(1000, 1000)
		msgs2 := q2.Poll(1000, 1000)
		for _, msg := range msgs {
			val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
			val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
			val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
			script := fmt.Sprintf("tableInsert(objByName(`"+receive+", true), %s,%s,%s)",
				val0, val1, val2)
			_, err := pcConn.RunScript(script)
			AssertNil(err)
		}
		for _, msg := range msgs2 {
			val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
			val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
			val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
			script := fmt.Sprintf("tableInsert(objByName(`"+receive+", true), %s,%s,%s)",
				val0, val1, val2)
			_, err := pcConn.RunScript(script)
			AssertNil(err)
		}
		res, _ := pcConn.RunScript("res = select * from " + receive + " where tag between 1:1000 order by tag;ex = select * from " + st + " where tag between 1:1000 order by tag;each(eqObj, ex.values(), res.values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}
		res, _ = pcConn.RunScript("res = select * from " + receive + " where tag between 2001:3000 order by tag;ex = select * from " + st + " where tag between 2001:3000 order by tag;each(eqObj, ex.values(), res.values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}

		err = pc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		err = pc.UnSubscribe(req2)
		So(err, ShouldBeNil)
		ClearStreamTable(host5, st)
		ClearStreamTable(host5, receive)

	})
	pc.Close()
	assert.True(t, pc.IsClosed())
}

func TestPollingClient_subscribe_unsubscribeesubscribe(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)
	Convey("TestPollingClient_subscribe_unsubscribeesubscribe", t, func() {
		st, receive := CreateStreamingTableWithRandomName(pcConn)
		req1 := &streaming.SubscribeRequest{
			Address:    host5,
			TableName:  st,
			ActionName: "sub" + st + "1",
			Offset:     0,
			Reconnect:  true,
		}
		q, err := pc.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = pcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		msgs := q.Poll(1000, 2000)
		actualows := 0
		for _, msg := range msgs {
			if msg != nil {
				actualows += 1
			}
		}
		So(actualows, ShouldEqual, 1000)
		err = pc.UnSubscribe(req1)
		So(err, ShouldBeNil)

		q, err = pc.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = pcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		msgs = q.Poll(1000, 2000)
		actualows = 0
		for _, msg := range msgs {
			if msg != nil {
				actualows += 1
			}
		}
		So(actualows, ShouldEqual, 2000)

		err = pc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		ClearStreamTable(host5, st)
		ClearStreamTable(host5, receive)
	})
	pc.Close()
	assert.True(t, pc.IsClosed())
}

func TestPollingClient_subscribe_AllowExists(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)
	Convey("TestPollingClient_subscribe_AllowExists", t, func() {
		st, receive := CreateStreamingTableWithRandomName(pcConn)
		filter1, err := pcConn.RunScript("1..1000")
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:     host5,
			TableName:   st,
			ActionName:  "sub" + st + "1",
			Offset:      0,
			Reconnect:   true,
			Filter:      filter1.(*model.Vector),
			AllowExists: true,
		}
		_, err = pc.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = pcConn.RunScript("n=2000;t1=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t1)")
		So(err, ShouldBeNil)

		q, err := pc.Subscribe(req)
		So(err, ShouldBeNil)
		msgs := q.Poll(1000, 1000)
		for _, msg := range msgs {
			val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
			val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
			val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
			script := fmt.Sprintf("tableInsert(objByName(`"+receive+", true), %s,%s,%s)",
				val0, val1, val2)
			_, err := pcConn.RunScript(script)
			AssertNil(err)
		}
		res, _ := pcConn.RunScript("res = select * from " + receive + " order by tag;ex = select * from " + st + " where tag between 1:1000 order by tag;each(eqObj, ex.values(), res.values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}
		err = pc.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
	pc.Close()
	assert.True(t, pc.IsClosed())
}

func TestPollingClient_subscribe_not_contain_handler(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)
	Convey("TestPollingClient_subscribe_not_contain_handler_1000", t, func() {
		st, receive := CreateStreamingTableWithRandomName(pcConn)
		req1 := &streaming.SubscribeRequest{
			Address:    host5,
			TableName:  st,
			ActionName: "sub" + st + "1",
			Offset:     -1,
			Reconnect:  true,
		}
		q, err := pc.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = pcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)
		msgs := q.Poll(1000, 2000)
		actualows := 0
		for _, msg := range msgs {
			if msg != nil {
				actualows += 1
			}
		}
		So(actualows, ShouldEqual, 1000)
		err = pc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		ClearStreamTable(host5, st)
		ClearStreamTable(host5, receive)

	})
	pc.Close()
	assert.True(t, pc.IsClosed())
}

func TestPollingClient_msgAsTable(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)
	Convey("TestPollingClient_msgAsTable", t, func() {
		st, receive := CreateStreamingTableWithRandomName(pcConn)
		req1 := &streaming.SubscribeRequest{
			Address:    host5,
			TableName:  st,
			ActionName: "sub" + st + "1",
			Offset:     0,
			Reconnect:  true,
			MsgAsTable: true,
		}
		req1.SetBatchSize(1000)
		q, err := pc.Subscribe(req1)
		So(err, ShouldBeNil)
		_, err = pcConn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "" + st + ".append!(t)")
		So(err, ShouldBeNil)

		msgs := q.Poll(1000, 2000)
		for _, msg := range msgs {
			val0 := msg.GetValue(0).(*model.Vector)
			val1 := msg.GetValue(1).(*model.Vector)
			val2 := msg.GetValue(2).(*model.Vector)

			for i := 0; i < len(val0.Data.Value()); i++ {
				script := fmt.Sprintf("tableInsert(objByName(`"+receive+", true), %s,%s,%s)",
					val0.Data.Get(i).String(), val1.Data.Get(i).String(), val2.Data.Get(i).String())
				_, err := pcConn.RunScript(script)
				AssertNil(err)
			}
		}
		res, _ := pcConn.RunScript("res = select * from " + receive + " order by tag;ex = select * from " + st + " order by tag;each(eqObj, ex.values(), res.values())")
		for _, val := range res.(*model.Vector).Data.Value() {
			So(val, ShouldBeTrue)
		}

		err = pc.UnSubscribe(req1)
		So(err, ShouldBeNil)
		ClearStreamTable(host5, st)
		ClearStreamTable(host5, receive)
	})
	pc.Close()
	assert.True(t, pc.IsClosed())
}

func TestPollingClient_subscribe_with_StreamDeserializer(t *testing.T) {
	var pc = streaming.NewPollingClient(setup.IP, setup.SubPort)

	Convey("TestPollingClient_subscribe_with_StreamDeserializer", t, func() {
		sdhandler, _ := createStreamDeserializer(pcConn, "SDoutTables_pc")
		req1 := &streaming.SubscribeRequest{
			Address:    host5,
			TableName:  "SDoutTables_pc",
			ActionName: "testStreamDeserializer",
			Offset:     0,
			Reconnect:  true,
		}

		targetows := 2000
		q, err := pc.Subscribe(req1)
		So(err, ShouldBeNil)
		fmt.Println("started subscribe...")

		msgs := q.Poll(1000, targetows)
		for _, msg := range msgs {
			ret, err := sdhandler.sd.Parse(msg)
			AssertNil(err)
			sym := ret.GetSym()
			if sym == "msg1" {
				AssertEqual(ret.Size(), 5)
				for i := 0; i < len(sdhandler.coltype1); i++ {
					AssertEqual(ret.GetValue(i).GetDataType(), sdhandler.coltype1[i])
					// fmt.Println(ret.GetValue(i).(*model.Scalar).Value())
					val := ret.GetValue(i).(*model.Scalar).Value()
					dt, err := model.NewDataType(sdhandler.coltype1[i], val)
					AssertNil(err)
					AssertNil(sdhandler.res1_data[i].Append(dt))
				}

			} else if sym == "msg2" {
				AssertEqual(ret.Size(), 4)
				for i := 0; i < len(sdhandler.coltype2); i++ {
					AssertEqual(ret.GetValue(i).GetDataType(), sdhandler.coltype2[i])
					// fmt.Println(ret.GetValue(i).GetDataType(), ex_types2[i])
					val := ret.GetValue(i).(*model.Scalar).Value()
					dt, err := model.NewDataType(sdhandler.coltype2[i], val)
					AssertNil(err)
					AssertNil(sdhandler.res2_data[i].Append(dt))
				}

			}
		}

		err = pc.UnSubscribe(req1)
		So(err, ShouldBeNil)

		res_tab1 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1", "price2"}, sdhandler.res1_data)
		res_tab2 := model.NewTable([]string{"datetimev", "timestampv", "sym", "price1"}, sdhandler.res2_data)

		pcConn.Upload(map[string]model.DataForm{"res1": res_tab1, "res2": res_tab2})
		ans1, err := pcConn.RunScript("res = select * from res1 order by datetimev,timestampv;ex= select * from table1 order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans1.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}

		ans2, err := pcConn.RunScript("res = select * from res2 order by datetimev,timestampv;ex= select * from table2 order by datetimev,timestampv;each(eqObj, res.values(), ex.values())")
		AssertNil(err)
		for _, ans := range ans2.(*model.Vector).Data.Value() {
			So(ans, ShouldBeTrue)
		}
		_, err = pcConn.RunScript(
			"try{ dropStreamTable(`SDoutTables_pc);}catch(ex){};" +
				"try{ dropStreamTable(`st2);}catch(ex){};" +
				"try{ undef(`table1, SHARED);}catch(ex){};" +
				"try{ undef(`table2, SHARED);}catch(ex){};go")
		So(err, ShouldBeNil)
	})
	pc.Close()
	assert.True(t, pc.IsClosed())
}
